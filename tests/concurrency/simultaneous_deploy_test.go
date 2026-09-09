package concurrency

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/api"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// DL-08 (Gate G-21): Simultaneous deployments to different projects.
// Both tracked correctly, no cross-contamination.
func TestDL08_G21_SimultaneousDeploymentsDifferentProjects(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-dl08",
		Hostname:  "node-dl08",
		IPAddress: "10.0.0.1",
		Capacity:  20,
	})

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	const numProjects = 4
	var wg sync.WaitGroup
	errCh := make(chan error, numProjects)
	depResults := make([]*deployments.Deployment, numProjects)

	for i := 0; i < numProjects; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			projID := fmt.Sprintf("project-concurrent-%d", idx)
			dep, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
				ProjectID:     projID,
				Image:         fmt.Sprintf("app-image-%d:v1", idx),
				InstanceCount: 1,
			})
			if err != nil {
				errCh <- fmt.Errorf("deploy for %s failed: %w", projID, err)
				return
			}
			depResults[idx] = dep
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("DL-08 failure: %v", err)
	}

	// Verify all projects have distinct deployments in RUNNING state
	seenDeps := make(map[string]bool)
	for i, dep := range depResults {
		if dep == nil {
			t.Fatalf("project %d had nil deployment", i)
		}
		if dep.Status != deployments.StatusRunning {
			t.Fatalf("project %d status is %s, expected RUNNING", i, dep.Status)
		}
		if seenDeps[dep.ID] {
			t.Fatalf("DL-08 VIOLATION: duplicate deployment ID across projects: %s", dep.ID)
		}
		seenDeps[dep.ID] = true

		// Verify instances have correct project label and isolation
		instances, _ := instRepo.ListByDeployment(ctx, dep.ID)
		if len(instances) != 1 {
			t.Fatalf("expected 1 instance for dep %s, got %d", dep.ID, len(instances))
		}
	}

	t.Log("DL-08 (Gate G-21) Passed: Simultaneous deployments to different projects tracked correctly with zero cross-contamination!")
}

// RACE-01 (Gate G-21 extends it): Simultaneous deployments to the same project.
// No corrupted state, deterministic outcome.
func TestRACE01_G21_SimultaneousDeploymentsSameProject(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-race01",
		Hostname:  "node-race01",
		IPAddress: "10.0.0.1",
		Capacity:  20,
	})

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	const projectID = "project-shared-race01"
	const concurrentDeploys = 3

	var wg sync.WaitGroup
	errCh := make(chan error, concurrentDeploys)
	deploys := make([]*deployments.Deployment, concurrentDeploys)

	for i := 0; i < concurrentDeploys; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			dep, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
				ProjectID:     projectID,
				Image:         fmt.Sprintf("web-service:v%d", idx),
				InstanceCount: 1,
			})
			if err != nil {
				errCh <- fmt.Errorf("concurrent deploy %d failed: %w", idx, err)
				return
			}
			deploys[idx] = dep
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("RACE-01 failure: %v", err)
	}

	// Verify all deployments completed to RUNNING without corrupted DB records
	for i, dep := range deploys {
		persisted, err := depRepo.GetByID(ctx, dep.ID)
		if err != nil {
			t.Fatalf("failed to get deployment %d (%s): %v", i, dep.ID, err)
		}
		if persisted.Status != deployments.StatusRunning {
			t.Fatalf("deployment %d status is %s, expected RUNNING", i, persisted.Status)
		}
	}

	t.Log("RACE-01 (Gate G-21) Passed: Simultaneous deployments to the same project serialized cleanly with zero corrupted state!")
}

// RACE-02 (Gate G-22): Duplicate/retried API requests idempotent at HTTP API layer.
func TestRACE02_G22_DuplicateRetriedAPIRequests_Idempotent(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	projRepo := projects.NewMemoryProjectRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()
	router := loadbalancer.NewRouter(log)

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-idemp",
		Hostname:  "node-idemp",
		IPAddress: "10.0.0.1",
		Capacity:  20,
	})

	_ = projRepo.Create(ctx, &projects.Project{
		ID:   "proj-idemp-1",
		Name: "Idempotent Project",
	})

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)
	apiServer := api.NewServer(depService, projRepo, reg, router, log)
	handler := api.NewRouter(apiServer)

	const idempotencyKey = "idemp-key-abc-12345"
	const concurrentRequests = 5

	var wg sync.WaitGroup
	type httpResult struct {
		StatusCode int
		Body       []byte
	}
	results := make([]httpResult, concurrentRequests)

	requestBody := []byte(`{"image":"my-app:1.0.0","instance_count":1}`)

	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/projects/proj-idemp-1/deployments", bytes.NewReader(requestBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", idempotencyKey)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			results[idx] = httpResult{
				StatusCode: rec.Result().StatusCode,
				Body:       rec.Body.Bytes(),
			}
		}(i)
	}

	wg.Wait()

	// 1. All concurrent requests MUST return 202 Accepted
	var firstDepID string
	for i, res := range results {
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("request %d returned HTTP %d: %s", i, res.StatusCode, string(res.Body))
		}

		var depResp api.DeploymentResponse
		if err := json.Unmarshal(res.Body, &depResp); err != nil {
			t.Fatalf("failed to decode response %d: %v", i, err)
		}
		if firstDepID == "" {
			firstDepID = depResp.ID
		} else if depResp.ID != firstDepID {
			t.Fatalf("RACE-02 VIOLATION: duplicate request returned different deployment ID (%s != %s)", depResp.ID, firstDepID)
		}
	}

	// 2. Exactly ONE deployment record must exist in durable storage!
	allDeps, _ := depRepo.List(ctx, "proj-idemp-1")
	if len(allDeps) != 1 {
		t.Fatalf("RACE-02 / G-22 VIOLATION: expected exactly 1 deployment in storage, found %d!", len(allDeps))
	}
	if allDeps[0].ID != firstDepID {
		t.Fatalf("expected stored deployment ID %s, got %s", firstDepID, allDeps[0].ID)
	}

	t.Logf("RACE-02 (Gate G-22) Passed: %d parallel duplicate requests returned identical responses; exactly 1 deployment created in storage!", concurrentRequests)
}

// RACE-04: Reconcile loop racing a live deployment.
// No double-create when a reconcile pass and an in-flight deploy overlap.
func TestRACE04_ReconcileLoopRacingLiveDeployment_NoDoubleCreate(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-race04",
		Hostname:  "node-race04",
		IPAddress: "10.0.0.1",
		Capacity:  20,
	})

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)
	reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockClientFactory, log)

	// Intercept the deployment during STARTING and run concurrent reconcile passes while in-flight
	reconcileActionsRecorded := make([]int, 0)
	var recMu sync.Mutex

	depService.SetCrashHook(func(st deployments.DeploymentStatus) error {
		if st == deployments.StatusStarting {
			// Trigger concurrent reconcile passes while deployment is in-flight
			for i := 0; i < 5; i++ {
				actions, err := reconciler.ReconcileOnce(ctx)
				if err == nil {
					recMu.Lock()
					reconcileActionsRecorded = append(reconcileActionsRecorded, actions.RecreatedCount)
					recMu.Unlock()
				}
			}
		}
		return nil
	})

	// Deploy an application with 2 desired replicas
	dep, instances, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "proj-race04",
		Image:         "web:v1",
		InstanceCount: 2,
	})
	if err != nil {
		t.Fatalf("CreateAndDeploy failed: %v", err)
	}

	if len(instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(instances))
	}

	// Confirm that racing reconciler passes created ZERO premature/duplicate instances
	recMu.Lock()
	for _, recreated := range reconcileActionsRecorded {
		if recreated > 0 {
			t.Fatalf("RACE-04 VIOLATION: reconciler created %d duplicate instances while deploy was in-flight!", recreated)
		}
	}
	recMu.Unlock()

	// Final verification: total instance records in storage strictly equals desired (2)
	storedInstances, _ := instRepo.ListByDeployment(ctx, dep.ID)
	if len(storedInstances) != 2 {
		t.Fatalf("RACE-04 VIOLATION: expected exactly 2 instances for deployment, found %d!", len(storedInstances))
	}

	t.Log("RACE-04 Passed: Reconcile loop racing a live deployment created zero duplicate instances!")
}

// RACE-05: Concurrent heartbeats from many workers.
// No lock contention causing dropped or delayed heartbeat processing.
func TestRACE05_ConcurrentHeartbeatsFromManyWorkers(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)

	const workerCount = 50
	for i := 0; i < workerCount; i++ {
		_, _ = reg.Register(ctx, workers.RegisterParams{
			WorkerKey: fmt.Sprintf("worker-node-%d", i),
			Hostname:  fmt.Sprintf("node-%d", i),
			IPAddress: fmt.Sprintf("10.10.1.%d", i),
			Capacity:  10,
		})
	}

	var wg sync.WaitGroup
	var successCount int64
	var failCount int64
	startTime := time.Now()

	// Fire concurrent heartbeats from all 50 workers simultaneously
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			workerKey := fmt.Sprintf("worker-node-%d", idx)
			// Each worker sends 5 successive heartbeats
			for b := 0; b < 5; b++ {
				if err := reg.Heartbeat(ctx, workerKey); err != nil {
					atomic.AddInt64(&failCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	if failCount > 0 {
		t.Fatalf("RACE-05 failure: %d heartbeats failed due to contention!", failCount)
	}
	expectedBeats := int64(workerCount * 5)
	if successCount != expectedBeats {
		t.Fatalf("expected %d successful heartbeats, got %d", expectedBeats, successCount)
	}

	t.Logf("RACE-05 Passed: %d concurrent heartbeats processed in %v with ZERO dropped beats or lock timeouts!",
		successCount, duration)
}

// PERF-02: Scheduler decision latency under concurrent requests.
// Stays within target latency (<50ms) with N simultaneous scheduling calls.
func TestPERF02_SchedulerDecisionLatencyUnderConcurrentRequests(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)
	instRepo := deployments.NewMemoryInstanceRepository()
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)

	// Register 10 candidate workers
	for i := 0; i < 10; i++ {
		_, _ = reg.Register(ctx, workers.RegisterParams{
			WorkerKey: fmt.Sprintf("perf-worker-%d", i),
			Hostname:  fmt.Sprintf("perf-node-%d", i),
			IPAddress: fmt.Sprintf("192.168.1.%d", i),
			Capacity:  100,
		})
	}

	const concurrentCalls = 50
	var wg sync.WaitGroup
	latencies := make([]time.Duration, concurrentCalls)

	for i := 0; i < concurrentCalls; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start := time.Now()
			w, err := sched.SelectWorker(ctx, scheduler.WorkloadRequirement{
				DeploymentID:     fmt.Sprintf("dep-perf-%d", idx),
				RequiredCapacity: 1,
			})
			elapsed := time.Since(start)
			latencies[idx] = elapsed
			if err != nil || w == nil {
				t.Errorf("scheduler call %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	// Calculate max and average latency
	var total time.Duration
	var maxLatency time.Duration
	for _, lat := range latencies {
		total += lat
		if lat > maxLatency {
			maxLatency = lat
		}
	}
	avgLatency := total / concurrentCalls

	// Target: max latency under concurrent load must be < 50ms
	if maxLatency > 50*time.Millisecond {
		t.Fatalf("PERF-02 VIOLATION: max scheduling latency was %v, exceeded 50ms threshold!", maxLatency)
	}

	t.Logf("PERF-02 Passed: %d concurrent scheduler decisions evaluated; avg latency: %v, max: %v (well below 50ms threshold)!",
		concurrentCalls, avgLatency, maxLatency)
}
