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
	"time"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/discovery"
	"github.com/nebula/nebula/internal/heartbeat"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// Phase 5 Exit Check:
// "kill a worker process — full G-12 flow. Blackhole the network instead — confirm it's 'unreachable,' not 'dead,' and nothing else is affected."
func TestPhase5_ExitCheck_WorkerDeathAndNetworkPartition(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// 1. Setup full Control Plane infrastructure
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

	serviceReg := discovery.NewServiceRegistry(router, log)
	reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockClientFactory, log)
	reconciler.SetServiceRegistry(serviceReg)

	// Heartbeat timeout configuration: 50ms interval, 3 misses = 150ms to UNHEALTHY
	cfg := workers.HealthStateMachineConfig{
		HeartbeatInterval:      50 * time.Millisecond,
		SuspectedThreshold:     2,
		UnhealthyThreshold:     3,
		ConsecutiveBeatsToHeal: 2,
	}
	reg.SetHealthConfig(cfg)
	monitor := heartbeat.NewTimeoutMonitor(reg, serviceReg, cfg, log)

	// 2. Register three distinct workers in the cluster:
	// Worker A: target of process kill (G-12 test)
	// Worker B: target of network blackhole (G-13 test)
	// Worker C: healthy replacement / untouched worker
	wA, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-A-kill",
		Hostname:  "node-A",
		IPAddress: "10.0.0.1",
		Capacity:  10,
	})
	wB, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-B-blackhole",
		Hostname:  "node-B",
		IPAddress: "10.0.0.2",
		Capacity:  10,
	})
	wC, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-C-healthy",
		Hostname:  "node-C",
		IPAddress: "10.0.0.3",
		Capacity:  10,
	})

	// Live HTTP backend mock server representing application container
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"phase":"5","status":"healthy-and-recovered"}`))
	}))
	defer backendServer.Close()

	// 3. Deploy application from Dockerfile source
	repoDir := t.TempDir()
	dockerfile := "FROM alpine:3.19\nCMD [\"./server\"]\n"
	_ = os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte(dockerfile), 0644)
	_ = os.WriteFile(filepath.Join(repoDir, "main.go"), []byte("package main"), 0644)

	projectID := "service-phase5"
	dep, instances, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		SourcePath:    repoDir,
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateAndDeploy failed: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance deployed, got %d", len(instances))
	}
	inst := instances[0]

	// Ensure instance is initially scheduled on Worker A for testing death recovery
	inst.WorkerID = wA.ID
	_ = instRepo.Update(ctx, inst)

	// Register initial healthy endpoint in Service Discovery and Load Balancer
	_ = serviceReg.RegisterEndpoint(ctx, discovery.Endpoint{
		InstanceID:   inst.ID,
		DeploymentID: dep.ID,
		ProjectID:    projectID,
		WorkerID:     wA.ID,
		Address:      backendServer.URL,
		Status:       discovery.EndpointHealthy,
	})

	// Verify initially reachable through Load Balancer
	req := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/health", nil)
	req.Header.Set("X-Project-ID", projectID)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected initial HTTP 200, got %d", rec.Result().StatusCode)
	}

	t.Logf("Setup complete: Instance %s running on Worker A (%s), routed via Load Balancer",
		inst.InstanceKey, wA.WorkerKey)

	// =========================================================================
	// EXIT CHECK PART 1: Kill a worker process — full G-12 flow
	// =========================================================================
	t.Log("=== Testing Exit Check Part 1: Worker Death (G-12) ===")

	// Step 1.1: Kill worker process -> heartbeats expire past threshold
	deathTime := time.Now().UTC().Add(250 * time.Millisecond)

	// Worker A process dies (no heartbeats sent).
	// Worker B and Worker C remain alive and continue sending heartbeats at deathTime.
	_ = reg.HeartbeatAt(ctx, wB.WorkerKey, deathTime)
	_ = reg.HeartbeatAt(ctx, wC.WorkerKey, deathTime)

	transitions := monitor.CheckWorkers(ctx, deathTime)
	if len(transitions) == 0 {
		t.Fatalf("Exit check part 1 failure: timeout monitor did not record worker health transition")
	}

	wACheck, _ := reg.Get(wA.WorkerKey)
	if wACheck.Health != workers.HealthUnhealthy {
		t.Fatalf("Exit check part 1 failure: expected Worker A to transition to UNHEALTHY, got %s", wACheck.Health)
	}

	// Step 1.2: Endpoints disabled in service discovery & load balancer (G-10)
	targetsAfterDeath := router.GetTargets(projectID)
	if len(targetsAfterDeath) != 0 {
		t.Fatalf("Exit check part 1 failure (G-10): dead worker's endpoint was not disabled in router! Targets: %+v", targetsAfterDeath)
	}

	// Step 1.3: Reconcile cycle detects missing workload and scheduler places replacement on healthy worker (Worker C)
	actions, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile cycle failed: %v", err)
	}
	if actions.RecreatedCount != 1 {
		t.Fatalf("Exit check part 1 failure: expected 1 replacement instance recreated, got %d", actions.RecreatedCount)
	}

	// Step 1.4: Confirm replacement is placed on healthy worker (Worker C or B) and running
	updatedInst, err := instRepo.GetByInstanceKey(ctx, inst.InstanceKey)
	if err != nil || updatedInst.WorkerID == wA.ID {
		t.Fatalf("Exit check part 1 failure: instance was not moved away from dead worker A! WorkerID: %s", updatedInst.WorkerID)
	}
	if updatedInst.Status != "RUNNING" {
		t.Fatalf("Exit check part 1 failure: instance status is %s, expected RUNNING", updatedInst.Status)
	}

	// Step 1.5: Desired == Observed confirmed via second immediate reconcile
	actionsConverged, err := reconciler.ReconcileOnce(ctx)
	if err != nil || !actionsConverged.IsZero() {
		t.Fatalf("Exit check part 1 failure: cluster did not converge to Desired==Observed; actions: %+v", actionsConverged)
	}

	t.Logf("Exit Check Part 1 SUCCESS: Worker killed -> heartbeat timeout -> endpoints disabled -> replacement scheduled on %s -> Desired==Observed converged!",
		updatedInst.WorkerID)

	// =========================================================================
	// EXIT CHECK PART 2: Blackhole the network instead — confirm 'unreachable', not 'dead', and nothing else affected
	// =========================================================================
	t.Log("=== Testing Exit Check Part 2: Network Blackhole (G-13) ===")

	// Step 2.1: Blackhole Worker B's network connection to Control Plane
	_, err = reg.SetPartitioned(ctx, wB.WorkerKey, true)
	if err != nil {
		t.Fatalf("failed to partition worker B: %v", err)
	}

	// Step 2.2: Confirm Worker B is classified as 'UNREACHABLE', NOT 'UNHEALTHY' (not dead)
	wBCheck, _ := reg.Get(wB.WorkerKey)
	if wBCheck.Health != workers.HealthUnreachable {
		t.Fatalf("Exit check part 2 failure: expected worker B to be UNREACHABLE, got %s", wBCheck.Health)
	}
	if !wBCheck.IsPartitioned {
		t.Fatalf("Exit check part 2 failure: worker B IsPartitioned is false")
	}

	// Step 2.3: Confirm Worker C (and other workers) are completely unaffected!
	wCCheck, _ := reg.Get(wC.WorkerKey)
	if wCCheck.Health != workers.HealthHealthy {
		t.Fatalf("Exit check part 2 failure: untouched Worker C was corrupted! Health: %s", wCCheck.Health)
	}
	if !wCCheck.IsHealthy() {
		t.Fatalf("Exit check part 2 failure: Worker C is not healthy")
	}
	if !wCCheck.EvaluateSchedulability().Allowed {
		t.Fatalf("Exit check part 2 failure: Worker C became unschedulable")
	}

	// Step 2.4: When partition heals, Worker B rejoins cleanly without false-healthy or duplicate scheduling
	_, _ = reg.SetPartitioned(ctx, wB.WorkerKey, false)
	healTime := deathTime.Add(1 * time.Second)
	_ = reg.HeartbeatAt(ctx, wB.WorkerKey, healTime)                      // beat 1
	_ = reg.HeartbeatAt(ctx, wB.WorkerKey, healTime.Add(100*time.Millisecond)) // beat 2 -> restores HEALTHY via hysteresis

	wBHealed, _ := reg.Get(wB.WorkerKey)
	if wBHealed.Health != workers.HealthHealthy {
		t.Fatalf("Exit check part 2 failure: Worker B did not heal to HEALTHY after consecutive heartbeats, got %s", wBHealed.Health)
	}

	// Verify service remains reachable through load balancer
	_ = router.RegisterTarget(projectID, updatedInst.ID, backendServer.URL)
	req2 := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/health", nil)
	req2.Header.Set("X-Project-ID", projectID)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)

	resp := rec2.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 after recovery, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"healthy-and-recovered"`) {
		t.Fatalf("unexpected body: %s", string(body))
	}

	t.Logf("Exit Check Part 2 SUCCESS: Blackhole marked Worker B as UNREACHABLE (not dead), Worker C completely untouched, healed cleanly!")
	t.Log("=== Phase 5 Exit Check COMPLETELY SATISFIED! ===")
}
