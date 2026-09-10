package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// TestRegistryLifecycle_BuildPushAndReleaseRecord verifies that:
// 1. Build Orchestrator pushes built image to test OCI registry.
// 2. Verified immutable sha256 digest is persisted in deployments and releases tables (§10.1).
func TestRegistryLifecycle_BuildPushAndReleaseRecord(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	// 1. Stand up real in-process OCI v2 distribution registry server
	regServer := registry.NewEmbeddedRegistryServer(log)
	if err := regServer.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("start embedded OCI registry: %v", err)
	}
	defer regServer.Close()

	regClient := registry.NewRemoteRegistryClient(registry.RemoteRegistryConfig{
		RegistryHost: regServer.Addr(),
		Insecure:     true,
	}, nil, log)

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	relRepo := deployments.NewMemoryReleaseRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	clientFactory := deployments.NewMockWorkerClientFactory()

	// Register worker node
	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-worker-1",
		Capacity:  10,
	})

	svc := deployments.NewService(depRepo, instRepo, reg, sched, clientFactory, log)
	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	router := loadbalancer.NewRouter(log)
	svc.SetBuildAndRegistry(orchestrator, regClient, router)
	svc.SetReleaseRepo(relRepo)

	// Signing keys
	privKey, _, _ := registry.GenerateSigningKeyPair()
	signer := registry.NewEd25519ImageSigner(privKey)
	svc.SetSigner(signer)

	// Prepare app source
	srcDir := filepath.Join(t.TempDir(), "app-lifecycle")
	_ = os.MkdirAll(srcDir, 0755)
	_ = os.WriteFile(filepath.Join(srcDir, "Dockerfile"), []byte("FROM alpine:latest\nEXPOSE 8080\nCMD [\"app\"]"), 0644)

	projID := "proj-lifecycle-test"
	dep, insts, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:  projID,
		SourcePath: srcDir,
	})
	if err != nil {
		t.Fatalf("CreateAndDeploy failed: %v", err)
	}

	if dep.Status != deployments.StatusRunning {
		t.Fatalf("expected status RUNNING, got %s", dep.Status)
	}
	if dep.ImageDigest == "" {
		t.Fatalf("expected image_digest to be recorded in deployment record")
	}

	// Verify release record
	releases, err := relRepo.ListByProject(ctx, projID)
	if err != nil || len(releases) == 0 {
		t.Fatalf("expected release record in releases table, got %d (err: %v)", len(releases), err)
	}
	rel := releases[0]
	if rel.ImageDigest != dep.ImageDigest {
		t.Fatalf("release image_digest mismatch: want %s, got %s", dep.ImageDigest, rel.ImageDigest)
	}
	if rel.Signature == "" {
		t.Fatalf("expected cryptographic signature recorded in release")
	}
	if len(insts) == 0 {
		t.Fatalf("expected deployed instance")
	}
}

// TestRegistryLifecycle_WorkerPullByDigest verifies that a decoupled worker agent
// pulls by digest from a real OCI registry on cache miss and runs the container.
func TestRegistryLifecycle_WorkerPullByDigest(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	regServer := registry.NewEmbeddedRegistryServer(log)
	if err := regServer.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("start registry server: %v", err)
	}
	defer regServer.Close()

	regClient := registry.NewRemoteRegistryClient(registry.RemoteRegistryConfig{
		RegistryHost: regServer.Addr(),
		Insecure:     true,
	}, nil, log)

	// Push test image
	tag := "nebula/proj-digest-pull:v1"
	payload := []byte("image-data-digest-pull")
	expectedDigest := registry.ComputeDigest(payload)
	digest, err := regClient.Push(ctx, tag, payload, expectedDigest)
	if err != nil {
		t.Fatalf("push to registry: %v", err)
	}

	// Worker Agent starts with empty local image cache
	mockDocker := runtime.NewMockDockerClient()
	mockDocker.ImageCache = make(map[string]bool)
	mockDocker.Registry = regClient

	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(mockDocker, tracker, log)

	// Worker receives RunContainer with digest
	runRes, err := ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   "inst-worker-pull-01",
		DeploymentID: "dep-01",
		Image:        tag,
		Labels: map[string]string{
			"nebula.image_digest": digest,
		},
	})
	if err != nil {
		t.Fatalf("RunContainer failed on pull by digest: %v", err)
	}

	if runRes.Status != string(runtime.StateRunning) {
		t.Fatalf("expected StateRunning, got %s", runRes.Status)
	}
	if mockDocker.PullCalls != 1 {
		t.Fatalf("expected exactly 1 pull call from registry on cache miss, got %d", mockDocker.PullCalls)
	}
	if !mockDocker.ImageCache[tag] {
		t.Fatalf("expected image to be populated in worker cache after pull")
	}
}

// TestRegistryLifecycle_DigestImmutabilityAcrossRetagging verifies that retagging
// an image tag to a different payload never changes what an existing deployment redeploys as (§10.1).
func TestRegistryLifecycle_DigestImmutabilityAcrossRetagging(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()
	reg := registry.NewMemoryRegistry(log)

	// Step 1: Deploy v17 with original payload and record digest D1
	tag := "nebula/app:v17"
	originalPayload := []byte("original-production-service-v17-code")
	d1, err := reg.Push(ctx, tag, originalPayload, "")
	if err != nil {
		t.Fatalf("initial push: %v", err)
	}

	// Deployment records digest d1
	recordedDeployment := &deployments.Deployment{
		ID:          uuid.New().String(),
		ProjectID:   "proj-immutability",
		Image:       tag,
		ImageDigest: d1,
		Status:      deployments.StatusRunning,
	}

	// Step 2: Malicious or accidental retag of v17 with totally different contents D2
	modifiedPayload := []byte("overwritten-tampered-v17-code-danger")
	d2, err := reg.Push(ctx, tag, modifiedPayload, "")
	if err != nil {
		t.Fatalf("retag push: %v", err)
	}
	if d1 == d2 {
		t.Fatalf("digests should differ for different payloads")
	}

	// Step 3: Redeploy uses recorded ImageDigest D1, NOT the mutable tag
	// Worker Agent checks expected digest D1 against retrieved image
	workerDocker := runtime.NewMockDockerClient()
	workerDocker.ImageCache = make(map[string]bool)
	workerDocker.Registry = reg
	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(workerDocker, tracker, log)

	// Attempting to pull the mutable tag when expecting D1 detects digest mismatch
	_, err = ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   "inst-redeploy-01",
		DeploymentID: recordedDeployment.ID,
		Image:        recordedDeployment.Image,
		Labels: map[string]string{
			"nebula.image_digest": recordedDeployment.ImageDigest, // expects d1
		},
	})

	// The registry returned d2 for tag v17, which doesn't match expected d1!
	// Worker strictly rejects the mismatch, preventing supply-chain poisoning.
	if err == nil {
		t.Fatalf("expected digest mismatch error when retagged image was pulled, got nil")
	}
	if workerDocker.CreateCalls != 0 {
		t.Fatalf("container must not be created when pulled digest doesn't match recorded digest")
	}
}

// TestRegistryLifecycle_GarbageCollectionIsolation verifies that running deployments
// remain completely unaffected when old unreferenced images are garbage-collected (§15).
func TestRegistryLifecycle_GarbageCollectionIsolation(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()
	reg := registry.NewMemoryRegistry(log)

	// Push 3 versions
	activeDigest, _ := reg.Push(ctx, "nebula/app:v2-active", []byte("active-v2-code"), "")
	_, _ = reg.Push(ctx, "nebula/app:v1-old", []byte("old-v1-code"), "")
	_, _ = reg.Push(ctx, "nebula/app:v0-scratch", []byte("scratch-code"), "")

	// Run active deployment using activeDigest
	activeWorker := runtime.NewMockDockerClient()
	activeWorker.Registry = reg
	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(activeWorker, tracker, log)

	res, err := ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   "inst-active",
		DeploymentID: "dep-active",
		Image:        "nebula/app:v2-active",
		Labels: map[string]string{
			"nebula.image_digest": activeDigest,
		},
	})
	if err != nil || res.Status != string(runtime.StateRunning) {
		t.Fatalf("failed to start active container: %v", err)
	}

	// Trigger GC, retaining only activeDigest
	gcRes, err := reg.GarbageCollect(ctx, []string{activeDigest})
	if err != nil {
		t.Fatalf("GarbageCollect failed: %v", err)
	}
	if gcRes.DeletedImages != 2 {
		t.Fatalf("expected 2 stale images deleted, got %d", gcRes.DeletedImages)
	}
	if gcRes.RetainedImages != 1 {
		t.Fatalf("expected 1 active image retained, got %d", gcRes.RetainedImages)
	}

	// Active deployment image can still be queried/pulled
	if !reg.HasImage(ctx, "nebula/app:v2-active") {
		t.Fatalf("active image was erroneously garbage-collected")
	}
	dg, err := reg.GetDigest(ctx, "nebula/app:v2-active")
	if err != nil || dg != activeDigest {
		t.Fatalf("active image digest corrupt after GC: %v", err)
	}

	// Stale images are confirmed deleted
	if reg.HasImage(ctx, "nebula/app:v1-old") || reg.HasImage(ctx, "nebula/app:v0-scratch") {
		t.Fatalf("stale images still present after GC")
	}
}
