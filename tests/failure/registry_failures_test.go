package failure

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

type failingPushRegistry struct {
	registry.RegistryClient
	err error
}

func (f *failingPushRegistry) Push(ctx context.Context, tag string, data []byte, expectedDigest string) (string, error) {
	return "", f.err
}

// TestRegistryFailure_PushFailsDeploymentAborted verifies that when a push fails,
// the deployment does NOT proceed as if the image exists (§10.2, §15).
func TestRegistryFailure_PushFailsDeploymentAborted(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	baseReg := registry.NewMemoryRegistry(log)
	pushErr := errors.New("registry quota exceeded (507 Insufficient Storage)")
	failingReg := &failingPushRegistry{
		RegistryClient: baseReg,
		err:            pushErr,
	}

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()

	_, _ = reg.Register(ctx, workers.RegisterParams{WorkerKey: "w1", Capacity: 10})

	svc := deployments.NewService(depRepo, instRepo, reg, sched, mockFactory, log)
	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	router := loadbalancer.NewRouter(log)
	svc.SetBuildAndRegistry(orchestrator, failingReg, router)

	srcDir := filepath.Join(t.TempDir(), "app-push-fail")
	_ = os.MkdirAll(srcDir, 0755)
	_ = os.WriteFile(filepath.Join(srcDir, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"app\"]"), 0644)

	dep, insts, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:  "proj-push-fail",
		SourcePath: srcDir,
	})

	if err == nil {
		t.Fatalf("expected CreateAndDeploy to fail when registry push fails, got nil")
	}

	if dep.Status != deployments.StatusFailed {
		t.Fatalf("expected deployment status FAILED, got %s", dep.Status)
	}
	if dep.Stage != "REGISTRY_PUSH_FAILED" {
		t.Fatalf("expected stage REGISTRY_PUSH_FAILED, got %s", dep.Stage)
	}
	if len(insts) != 0 {
		t.Fatalf("expected 0 instances created when push failed, got %d", len(insts))
	}
	if len(mockFactory.Dispatched) != 0 {
		t.Fatalf("expected 0 containers dispatched to workers, got %d", len(mockFactory.Dispatched))
	}
}

// TestRegistryFailure_PullFailsOnFreshWorker verifies that when a fresh worker with
// no local cache fails to pull an image, it surfaces the error as a first-class failure event (§10.2).
func TestRegistryFailure_PullFailsOnFreshWorker(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	workerClient := runtime.NewMockDockerClient()
	workerClient.ImageCache = make(map[string]bool) // Fresh worker with empty cache
	workerClient.FailPull = errors.New("blob download corrupted: unexpected EOF (§10.2)")

	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(workerClient, tracker, log)

	res, err := ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   "inst-pull-fail-01",
		DeploymentID: "dep-pull-fail",
		Image:        "nebula/app:v1",
		Labels: map[string]string{
			"nebula.image_digest": "sha256:111122223333",
		},
	})

	if err == nil {
		t.Fatalf("expected RunContainer to fail on worker pull failure, got nil")
	}

	if res.Status != string(runtime.StateFailed) {
		t.Fatalf("expected status FAILED, got %s", res.Status)
	}

	if workerClient.CreateCalls != 0 {
		t.Fatalf("CreateContainer must not be called when pull fails, got %d", workerClient.CreateCalls)
	}

	if !strings.Contains(res.Error, "failed to pull image") {
		t.Fatalf("expected error to surface pull failure, got: %s", res.Error)
	}
}

// TestRegistryFailure_AuthFailureActionableEvent verifies that registry authentication
// failure (401/403) surfaces an actionable error event, avoiding silent hangs or retries (§10.2).
func TestRegistryFailure_AuthFailureActionableEvent(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	// Server returning HTTP 401 Unauthorized
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	regClient := registry.NewRemoteRegistryClient(registry.RemoteRegistryConfig{
		RegistryHost: host,
		Username:     "invalid",
		Password:     "credentials",
		Insecure:     true,
	}, nil, log)

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()

	svc := deployments.NewService(depRepo, instRepo, reg, sched, mockFactory, log)
	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	router := loadbalancer.NewRouter(log)
	svc.SetBuildAndRegistry(orchestrator, regClient, router)

	srcDir := filepath.Join(t.TempDir(), "app-auth-fail")
	_ = os.MkdirAll(srcDir, 0755)
	_ = os.WriteFile(filepath.Join(srcDir, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"app\"]"), 0644)

	start := time.Now()
	dep, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:  "proj-auth-fail",
		SourcePath: srcDir,
	})
	duration := time.Since(start)

	// Ensure it didn't hang
	if duration > 10*time.Second {
		t.Fatalf("auth failure hung for %v; expected immediate error return", duration)
	}

	if err == nil {
		t.Fatalf("expected deployment to fail on 401 registry auth failure")
	}

	if dep.Status != deployments.StatusFailed {
		t.Fatalf("expected status FAILED, got %s", dep.Status)
	}

	errMsg := strings.ToLower(err.Error())
	if !strings.Contains(errMsg, "authentication") && !strings.Contains(errMsg, "401") && !strings.Contains(errMsg, "unauthorized") {
		t.Fatalf("expected actionable authentication error message, got: %v", err)
	}
}

// TestRegistryFailure_WorkerPullAuthFailure_DistinctActionableError verifies that when a
// worker's registry credentials are expired or revoked at image pull time (§10.2), the worker
// surfaces a distinct, actionable error (REGISTRY_AUTH_FAILED) rather than a generic IMAGE_PULL_FAILED.
func TestRegistryFailure_WorkerPullAuthFailure_DistinctActionableError(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	workerClient := runtime.NewMockDockerClient()
	workerClient.ImageCache = make(map[string]bool) // Fresh worker with empty local cache
	// Worker registry pull credentials revoked/expired
	workerClient.FailPull = errors.New("401 Unauthorized: token expired or invalid registry credentials (§10.2)")

	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(workerClient, tracker, log)

	res, err := ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   "inst-pull-auth-fail",
		DeploymentID: "dep-pull-auth-fail",
		Image:        "nebula/secure-app:v1.0",
		Labels: map[string]string{
			"nebula.image_digest": "sha256:deadbeef12345678",
		},
	})

	if err == nil {
		t.Fatalf("expected RunContainer to fail when worker cannot authenticate to registry")
	}

	if res.Status != string(runtime.StateFailed) {
		t.Fatalf("expected instance state FAILED, got %s", res.Status)
	}

	if workerClient.CreateCalls != 0 {
		t.Fatalf("CreateContainer must not be called when registry authentication fails, got %d calls", workerClient.CreateCalls)
	}

	// Must surface as distinct, actionable REGISTRY_AUTH_FAILED error, not generic IMAGE_PULL_FAILED
	if !strings.Contains(res.Error, "REGISTRY_AUTH_FAILED") {
		t.Fatalf("expected error to explicitly identify REGISTRY_AUTH_FAILED, got: %s", res.Error)
	}
	if !strings.Contains(res.Error, "credentials rejected or expired") && !strings.Contains(res.Error, "401") {
		t.Fatalf("expected actionable diagnostic details in error message, got: %s", res.Error)
	}

	// Verify the instance tracker recorded the distinct error
	rec, exists := tracker.Get("inst-pull-auth-fail")
	if !exists {
		t.Fatalf("expected instance record to exist in tracker")
	}
	if !strings.Contains(rec.LastError, "REGISTRY_AUTH_FAILED") {
		t.Fatalf("expected tracker LastError to record REGISTRY_AUTH_FAILED, got: %s", rec.LastError)
	}
}
