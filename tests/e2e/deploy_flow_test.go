package e2e

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// DL-01 (G-05) & OW-01: Happy-path deploy from source to reachable running service.
// Pipeline: Source → build → registry → schedule → run → register → RUNNING + reachable through load balancer.
func TestDL01_OW01_HappyPathDeployFlowAndLoadBalancer(t *testing.T) {
	log := zerolog.Nop()
	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()
	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	regClient := registry.NewMemoryRegistry(log)
	router := loadbalancer.NewRouter(log)
	orchestrator := build.NewOrchestrator(build.NewEphemeralSandbox(log), log)
	depService.SetBuildAndRegistry(orchestrator, regClient, router)

	ctx := context.Background()

	// 1. Start a mock HTTP backend server representing the container workload instance
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy","service":"web-app","version":"1.0.0"}`))
	}))
	defer backendServer.Close()

	// Extract backend host and port
	backendURL := backendServer.URL // e.g. http://127.0.0.1:54321
	hostPort := strings.TrimPrefix(backendURL, "http://")
	parts := strings.Split(hostPort, ":")
	backendIP := parts[0]

	// 2. Register worker where instance will be scheduled
	workerKey := "worker-prod-1"
	w, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: workerKey,
		Hostname:  "prod-node-1",
		IPAddress: backendIP,
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// 3. Create sample source repository (Go project with go.mod)
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module github.com/example/webapp\ngo 1.22"), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package main\nfunc main(){}"), 0644)

	// 4. Trigger deployment from source (DL-01 / G-05)
	projectID := "my-awesome-app"
	dep, instances, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		SourcePath:    tmpDir,
		InstanceCount: 1,
		Env: map[string]string{
			"APP_ENV": "production",
		},
	})
	if err != nil {
		t.Fatalf("CreateAndDeploy failed: %v", err)
	}

	// -------------------------------------------------------------
	// Verification: DL-01 (G-05) Pipeline Stages
	// -------------------------------------------------------------
	// Stage 1: Build & Digest
	if dep.Image == "" {
		t.Fatalf("DL-01 failure: deployment image was not generated")
	}
	if dep.ImageDigest == "" || !strings.HasPrefix(dep.ImageDigest, "sha256:") {
		t.Fatalf("DL-01 failure: verified image digest missing: %s", dep.ImageDigest)
	}

	// Stage 2: Registry contains the pushed image and verified digest (BLD-07)
	if !regClient.HasImage(ctx, dep.Image) {
		t.Fatalf("DL-01 failure: image %s was not pushed to registry", dep.Image)
	}
	storedDigest, err := regClient.GetDigest(ctx, dep.Image)
	if err != nil || storedDigest != dep.ImageDigest {
		t.Fatalf("DL-01 failure: registry digest mismatch: %s vs %s", storedDigest, dep.ImageDigest)
	}

	// Stage 3: Scheduling & Worker Run
	if len(instances) != 1 {
		t.Fatalf("DL-01 failure: expected 1 instance, got %d", len(instances))
	}
	inst := instances[0]
	if inst.Status != "RUNNING" {
		t.Fatalf("DL-01 failure: instance status is %s, expected RUNNING", inst.Status)
	}
	if inst.WorkerID != w.ID {
		t.Fatalf("DL-01 failure: expected worker ID %s, got %s", w.ID, inst.WorkerID)
	}
	if len(mockClientFactory.Dispatched) != 1 {
		t.Fatalf("DL-01 failure: worker RunContainer was not dispatched")
	}

	// Stage 4: Deployment status is RUNNING
	if dep.Status != deployments.StatusRunning {
		t.Fatalf("DL-01 failure: deployment status is %s, expected RUNNING", dep.Status)
	}
	t.Logf("DL-01 (G-05) Passed: Source → build → registry (%s) → schedule → run (%s) → RUNNING!",
		dep.ImageDigest, inst.ContainerID)

	// -------------------------------------------------------------
	// Verification: OW-01 - Deployed instance reachable through Load Balancer
	// -------------------------------------------------------------
	// Register the actual running test backend in the load balancer for this instance
	_ = router.RegisterTarget(projectID, inst.ID, backendURL)

	// Send HTTP request to the load balancer service endpoint
	req := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/api/v1/health", nil)
	req.Header.Set("X-Project-ID", projectID)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	resp := recorder.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("OW-01 failure: expected HTTP 200 from load balancer, got %d", resp.StatusCode)
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)
	if !strings.Contains(bodyStr, `"service":"web-app"`) {
		t.Fatalf("OW-01 failure: unexpected response body from backend: %s", bodyStr)
	}

	forwardedBy := resp.Header.Get("X-Forwarded-By")
	if forwardedBy != "nebula-loadbalancer" {
		t.Fatalf("OW-01 failure: expected X-Forwarded-By header, got %s", forwardedBy)
	}

	t.Logf("OW-01 Passed: HTTP request through load balancer reached running instance! Response: %s", bodyStr)
}

// Exit Check: A real repo with a Dockerfile deploys end-to-end and becomes reachable.
// Pipeline: Repo with Dockerfile → Build Orchestrator (Dockerfile detected) → Ephemeral sandbox (no secrets)
//           → Container Registry (image + verified sha256 digest) → Scheduler → Worker RunContainer
//           → Load Balancer registration → Reachable over HTTP (200 OK).
func TestExitCheck_DockerfileDeployEndToEndAndReachable(t *testing.T) {
	log := zerolog.Nop()
	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()
	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	regClient := registry.NewMemoryRegistry(log)
	router := loadbalancer.NewRouter(log)
	orchestrator := build.NewOrchestrator(build.NewEphemeralSandbox(log), log)
	depService.SetBuildAndRegistry(orchestrator, regClient, router)

	ctx := context.Background()

	// 1. Worker registration
	workerKey := "worker-dockerfile-1"
	w, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: workerKey,
		Hostname:  "node-dockerfile-1",
		IPAddress: "192.168.1.100",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// 2. Real repo with a Dockerfile (and also package.json to prove Dockerfile takes precedence)
	repoDir := t.TempDir()
	dockerfileContent := `FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o server .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 8080
CMD ["./server"]
`
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte(dockerfileContent), 0644); err != nil {
		t.Fatalf("failed to create Dockerfile: %v", err)
	}
	// Add package.json to test that Dockerfile overrides zero-config auto-detection
	_ = os.WriteFile(filepath.Join(repoDir, "package.json"), []byte(`{"name":"override-me"}`), 0644)
	_ = os.WriteFile(filepath.Join(repoDir, "main.go"), []byte("package main\nfunc main(){}"), 0644)

	// 3. Mock backend HTTP service that the containerized app runs
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"running","app":"dockerfile-app","version":"v1"}`))
	}))
	defer backendServer.Close()

	// 4. Trigger deployment
	projectID := "dockerfile-prod-project"
	dep, instances, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		SourcePath:    repoDir,
		InstanceCount: 1,
		Env: map[string]string{
			"PORT": "8080",
		},
	})
	if err != nil {
		t.Fatalf("CreateAndDeploy failed for Dockerfile repo: %v", err)
	}

	// 5. Verify image was built and digest recorded
	if dep.Image == "" {
		t.Fatalf("Exit check failed: deployment image reference is empty")
	}
	if dep.ImageDigest == "" || !strings.HasPrefix(dep.ImageDigest, "sha256:") {
		t.Fatalf("Exit check failed: invalid or missing image digest: %s", dep.ImageDigest)
	}

	// 6. Verify image exists in registry with matching cryptographic digest
	if !regClient.HasImage(ctx, dep.Image) {
		t.Fatalf("Exit check failed: image %s not found in registry", dep.Image)
	}
	storedDigest, err := regClient.GetDigest(ctx, dep.Image)
	if err != nil || storedDigest != dep.ImageDigest {
		t.Fatalf("Exit check failed: registry digest %s does not match deployment digest %s", storedDigest, dep.ImageDigest)
	}

	// 7. Verify scheduled and running on worker
	if len(instances) != 1 {
		t.Fatalf("Exit check failed: expected 1 instance, got %d", len(instances))
	}
	inst := instances[0]
	if inst.Status != "RUNNING" {
		t.Fatalf("Exit check failed: instance status is %s, expected RUNNING", inst.Status)
	}
	if inst.WorkerID != w.ID {
		t.Fatalf("Exit check failed: expected instance placed on worker %s, got %s", w.ID, inst.WorkerID)
	}

	// 8. Register the backend target and verify reachable through load balancer
	if err := router.RegisterTarget(projectID, inst.ID, backendServer.URL); err != nil {
		t.Fatalf("Exit check failed: failed to register target in router: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/status", nil)
	req.Header.Set("X-Project-ID", projectID)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Exit check failed: expected HTTP 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"app":"dockerfile-app"`) {
		t.Fatalf("Exit check failed: unexpected response: %s", string(body))
	}

	t.Logf("Phase 3 Exit Check SUCCESS: A real repo with a Dockerfile deploys end-to-end and becomes reachable! Response: %s", string(body))
}

