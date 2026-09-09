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
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// Phase 4 Exit Check:
// "kill a container by hand outside Nebula; next reconcile cycle brings it back; running reconcile again immediately does nothing."
func TestPhase4_ExitCheck_ExternalKill_SelfHeal_IdempotentConvergence(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// 1. Core infrastructure repositories
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

	reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockClientFactory, log)

	// 2. Register a cluster worker
	workerKey := "worker-exit-check-4"
	w, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: workerKey,
		Hostname:  "node-phase4-exit",
		IPAddress: "10.10.4.1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// 3. Source repository with Dockerfile
	repoDir := t.TempDir()
	dockerfileContent := "FROM alpine:3.19\nCMD [\"./server\"]\n"
	_ = os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte(dockerfileContent), 0644)
	_ = os.WriteFile(filepath.Join(repoDir, "main.go"), []byte("package main"), 0644)

	// Mock live backend server for load balancer
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"phase":"4","status":"healed-and-alive"}`))
	}))
	defer backendServer.Close()

	// 4. Initial Deploy: Source -> build -> registry -> schedule -> run
	projectID := "phase4-exit-service"
	dep, instances, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		SourcePath:    repoDir,
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateAndDeploy failed: %v", err)
	}
	if dep.Status != deployments.StatusRunning {
		t.Fatalf("expected deployment status %s, got %s", deployments.StatusRunning, dep.Status)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 deployed instance, got %d", len(instances))
	}
	inst := instances[0]

	containerID := inst.ContainerID

	// Register in load balancer and confirm reachable initially
	_ = router.RegisterTarget(projectID, inst.ID, backendServer.URL)
	req := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/health", nil)
	req.Header.Set("X-Project-ID", projectID)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected initial HTTP 200, got %d", rec.Result().StatusCode)
	}

	t.Logf("Initial deploy succeeded: container %s is running for instance %s", containerID, inst.InstanceKey)

	// -------------------------------------------------------------
	// Exit Check Step 1: Kill a container by hand outside Nebula (e.g. docker rm -f)
	// -------------------------------------------------------------
	removed := mockClientFactory.RemoveContainer(w.ID, containerID)
	if !removed {
		t.Fatalf("failed to remove container outside Nebula")
	}

	// Verify the container is dead/missing from worker observation
	client, _ := mockClientFactory.GetClient(ctx, w)
	observedBefore, _ := client.ListContainers(ctx, nil)
	if len(observedBefore.Containers) != 0 {
		t.Fatalf("expected 0 containers on worker after external kill, got %d", len(observedBefore.Containers))
	}

	t.Logf("Simulated external manual deletion: container %s killed by hand outside Nebula", containerID)

	// -------------------------------------------------------------
	// Exit Check Step 2: Next reconcile cycle brings it back (self-healing)
	// -------------------------------------------------------------
	actionsCycle1, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile cycle 1 failed: %v", err)
	}

	if actionsCycle1.RecreatedCount != 1 {
		t.Fatalf("Phase 4 Exit Check failure: expected 1 recreated instance, got %d", actionsCycle1.RecreatedCount)
	}

	// Verify container is back and running on the worker
	observedAfter, _ := client.ListContainers(ctx, nil)
	if len(observedAfter.Containers) != 1 {
		t.Fatalf("Phase 4 Exit Check failure: expected 1 container running after repair, got %d", len(observedAfter.Containers))
	}
	recreatedCtr := observedAfter.Containers[0]
	if recreatedCtr.InstanceId != inst.InstanceKey {
		t.Fatalf("expected recreated instance key %s, got %s", inst.InstanceKey, recreatedCtr.InstanceId)
	}

	// Verify database/repository status is RUNNING
	updatedInst, err := instRepo.GetByInstanceKey(ctx, inst.InstanceKey)
	if err != nil || updatedInst.Status != "RUNNING" {
		t.Fatalf("expected repository instance to be RUNNING, got %s", updatedInst.Status)
	}

	t.Logf("Reconcile cycle 1 succeeded: brought container back! (new container ID: %s)", recreatedCtr.ContainerId)

	// -------------------------------------------------------------
	// Exit Check Step 3: Running reconcile again immediately does NOTHING (idempotence)
	// -------------------------------------------------------------
	actionsCycle2, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile cycle 2 failed: %v", err)
	}

	if !actionsCycle2.IsZero() {
		t.Fatalf("Phase 4 Exit Check failure: immediate second reconcile was NOT a no-op! Actions: %+v", actionsCycle2)
	}

	// Verify service remains completely healthy and reachable through load balancer
	req2 := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/health", nil)
	req2.Header.Set("X-Project-ID", projectID)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	resp2 := rec2.Result()
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 after self-healing, got %d", resp2.StatusCode)
	}
	body, _ := io.ReadAll(resp2.Body)
	if !strings.Contains(string(body), `"healed-and-alive"`) {
		t.Fatalf("unexpected response body: %s", string(body))
	}

	t.Logf("Phase 4 Exit Check SUCCESS: Kill container outside Nebula -> next reconcile cycle brings it back -> immediate re-run does nothing -> reachable via HTTP (%s)!", string(body))
}
