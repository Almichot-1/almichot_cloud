package chaos

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// TestChaos_KillBuildWorkerMidBuild verifies that killing a Build Worker node mid-build
// causes the affected deployment's build step to fail cleanly to FAILED (BUILD_FAILED),
// does not hang, and does not affect other in-flight builds on other Build Workers.
func TestChaos_KillBuildWorkerMidBuild(t *testing.T) {
	log := zerolog.Nop()
	workerRepo := workers.NewMemoryWorkerRepository()
	workerReg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(workerReg, nil, log)
	buildPool := build.NewLocalBuildWorkerPool()

	// 1. Setup 2 dedicated Build Workers with Capacity 1
	nodeA := build.NewBuildWorkerNode(build.BuildWorkerNodeConfig{
		WorkerKey: "build-worker-chaos-A",
		Hostname:  "bld-a.internal",
		IPAddress: "10.0.1.10",
		Port:      9091,
		Capacity:  1,
	}, log)
	_, err := nodeA.Register(context.Background(), workerReg)
	if err != nil {
		t.Fatalf("failed to register node A: %v", err)
	}
	buildPool.RegisterWorker("build-worker-chaos-A", nodeA)

	nodeB := build.NewBuildWorkerNode(build.BuildWorkerNodeConfig{
		WorkerKey: "build-worker-chaos-B",
		Hostname:  "bld-b.internal",
		IPAddress: "10.0.1.11",
		Port:      9091,
		Capacity:  1,
	}, log)
	_, err = nodeB.Register(context.Background(), workerReg)
	if err != nil {
		t.Fatalf("failed to register node B: %v", err)
	}
	buildPool.RegisterWorker("build-worker-chaos-B", nodeB)

	// 2. Setup 1 runtime worker for container execution
	_, err = workerReg.Register(context.Background(), workers.RegisterParams{
		WorkerKey: "runtime-worker-01",
		Hostname:  "run-01.internal",
		IPAddress: "10.0.2.10",
		GRPCPort:  9090,
		Capacity:  10,
		Labels:    map[string]string{"capability": workers.CapabilityRuntime},
	})
	if err != nil {
		t.Fatalf("failed to register runtime worker: %v", err)
	}

	// 3. Setup Deployments Service with dedicated worker sandbox
	sandbox := build.NewWorkerSandbox(sched, workerReg, buildPool, log)
	orchestrator := build.NewOrchestrator(sandbox, log)

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	mockFactory := deployments.NewMockWorkerClientFactory()

	router := loadbalancer.NewRouter(log)
	depService := deployments.NewService(
		depRepo,
		instRepo,
		workerReg,
		sched,
		mockFactory,
		log,
	)
	depService.SetBuildAndRegistry(orchestrator, depService.RegistryClient(), router)

	// Prepare build source directories
	srcDirA := t.TempDir()
	_ = os.WriteFile(filepath.Join(srcDirA, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"echo\", \"build-A\"]"), 0644)

	srcDirB := t.TempDir()
	_ = os.WriteFile(filepath.Join(srcDirB, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"echo\", \"build-B\"]"), 0644)

	// Inject mid-build kill hook into nodeA: as soon as build starts, signal and kill nodeA
	killTriggered := make(chan struct{})
	var killOnce sync.Once
	nodeA.SetMidBuildHook(func() {
		killOnce.Do(func() {
			close(killTriggered)
		})
		time.Sleep(30 * time.Millisecond)
		nodeA.Kill()
	})

	var wg sync.WaitGroup
	var depAErr, depBErr error
	var depA, depB *deployments.Deployment
	start := time.Now()

	// Launch Build on Node A (will be killed mid-build)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error
		depA, _, err = depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
			ProjectID:     "proj-chaos-a",
			SourcePath:    srcDirA,
			InstanceCount: 1,
		})
		depAErr = err
	}()

	// Launch Build on Node B concurrently as soon as build A starts on node A
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-killTriggered

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error
		depB, _, err = depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
			ProjectID:     "proj-chaos-b",
			SourcePath:    srcDirB,
			InstanceCount: 1,
		})
		depBErr = err
	}()

	wg.Wait()
	elapsed := time.Since(start)

	// Assertions:
	// 1. Did not hang
	if elapsed > 4*time.Second {
		t.Fatalf("chaos test hung: elapsed %v", elapsed)
	}

	// 2. Deployment A failed cleanly to FAILED (BUILD_FAILED)
	if depAErr == nil {
		t.Fatalf("expected deployment A to fail due to worker kill mid-build, but succeeded")
	}
	if depA == nil || depA.Status != deployments.StatusFailed {
		t.Fatalf("expected deployment A status to be FAILED, got: %v", depA.Status)
	}
	if depA.Stage != "BUILD_FAILED" {
		t.Fatalf("expected deployment A stage to be BUILD_FAILED, got: %s", depA.Stage)
	}

	// 3. Deployment B on nodeB succeeded unaffected
	if depBErr != nil {
		t.Fatalf("deployment B was affected by node A kill and failed: %v", depBErr)
	}
	if depB == nil || (depB.Status != deployments.StatusRunning && depB.Status != deployments.StatusBuilt) {
		t.Fatalf("expected deployment B to succeed, got status: %v", depB.Status)
	}
	if depB.ImageDigest == "" || !strings.HasPrefix(depB.ImageDigest, "sha256:") {
		t.Fatalf("expected valid digest for deployment B, got: %s", depB.ImageDigest)
	}

	t.Logf("PASSED: Node A killed mid-build cleanly failed dep A to BUILD_FAILED in %v without hanging or affecting dep B!", elapsed)
}
