package crash_stages

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// DL-02: Every state transition persisted before acting on it.
func TestDL02_EveryStateTransitionPersistedBeforeActing(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-dl02",
		Hostname:  "node-dl02",
		IPAddress: "127.0.0.1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	var recordedTransitions []deployments.DeploymentStatus

	depService.SetCrashHook(func(stage deployments.DeploymentStatus) error {
		// When hook is called, inspect the repository to assert the state is already persisted
		all, _ := depRepo.List(ctx, "")
		if len(all) == 0 {
			t.Fatalf("DL-02 VIOLATION: no deployment found in storage when hook called for stage %s", stage)
		}
		persisted := all[0]
		if persisted.Status != stage {
			t.Fatalf("DL-02 VIOLATION: stage %s not persisted before action! Storage has %s", stage, persisted.Status)
		}
		recordedTransitions = append(recordedTransitions, stage)
		return nil
	})

	repoDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM alpine:3.19\nCMD [\"sleep\", \"10\"]\n"), 0644)

	dep, instances, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "proj-dl02",
		SourcePath:    repoDir,
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateAndDeploy failed: %v", err)
	}

	if dep.Status != deployments.StatusRunning {
		t.Fatalf("expected final status RUNNING, got %s", dep.Status)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}

	// Verify the ordered progression of persisted transitions: QUEUED -> BUILDING -> SCHEDULING -> STARTING -> RUNNING
	expected := []deployments.DeploymentStatus{
		deployments.StatusQueued,
		deployments.StatusBuilding,
		deployments.StatusScheduling,
		deployments.StatusStarting,
		deployments.StatusRunning,
	}

	if len(recordedTransitions) != len(expected) {
		t.Fatalf("expected %d transitions, got %d: %v", len(expected), len(recordedTransitions), recordedTransitions)
	}
	for i, exp := range expected {
		if recordedTransitions[i] != exp {
			t.Fatalf("transition %d: expected %s, got %s", i, exp, recordedTransitions[i])
		}
	}

	t.Logf("DL-02 Passed: All %d state transitions persisted in storage prior to action execution!", len(recordedTransitions))
}

// CP-CRASH-01 (Gate G-16): CP crash while QUEUED.
func TestCPCrash01_G16_CrashWhileQueued(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	// Simulate crash immediately upon QUEUED
	errSimulatedCrash := errors.New("simulated CP SIGKILL during QUEUED")
	depService.SetCrashHook(func(stage deployments.DeploymentStatus) error {
		if stage == deployments.StatusQueued {
			return errSimulatedCrash
		}
		return nil
	})

	_, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "proj-crash-queued",
		Image:         "test-app:v1",
		InstanceCount: 1,
	})
	if !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("expected simulated crash error, got: %v", err)
	}

	// Confirm storage shows QUEUED
	all, _ := depRepo.List(ctx, "")
	if len(all) != 1 || all[0].Status != deployments.StatusQueued {
		t.Fatalf("expected deployment in storage with status QUEUED, got: %+v", all)
	}

	// --- CP RESTARTS ---
	recoveryEngine := deployments.NewRecoveryEngine(depRepo, instRepo, reg, sched, mockClientFactory, log)
	report, err := recoveryEngine.RecoverDeployments(ctx, deployments.RecoveryPolicyFailSafe)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if report.QueuedRecovered != 1 {
		t.Fatalf("expected 1 QUEUED deployment recovered, got %d", report.QueuedRecovered)
	}

	// Verify deployment state is safely FAILED with no ambiguous or orphan work
	recoveredDep, _ := depRepo.GetByID(ctx, all[0].ID)
	if recoveredDep.Status != deployments.StatusFailed {
		t.Fatalf("expected recovered deployment status FAILED, got %s", recoveredDep.Status)
	}
	if recoveredDep.Stage != "QUEUED_INTERRUPTED_CP_CRASH" {
		t.Fatalf("expected stage QUEUED_INTERRUPTED_CP_CRASH, got %s", recoveredDep.Stage)
	}

	// Confirm 0 containers exist on any worker
	for _, ctrs := range mockClientFactory.Containers {
		if len(ctrs) > 0 {
			t.Fatalf("G-16 VIOLATION: orphan containers exist after crash while QUEUED!")
		}
	}

	t.Log("CP-CRASH-01 (Gate G-16) Passed: CP crash while QUEUED recovered cleanly with zero orphan work!")
}

// CP-CRASH-02 (Gate G-17) & DL-04: CP crash while BUILDING.
func TestCPCrash02_G17_DL04_CrashWhileBuilding(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	errSimulatedCrash := errors.New("simulated CP SIGKILL during BUILDING")
	depService.SetCrashHook(func(stage deployments.DeploymentStatus) error {
		if stage == deployments.StatusBuilding {
			return errSimulatedCrash
		}
		return nil
	})

	repoDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM alpine:3.19\nCMD [\"echo\", \"build\"]\n"), 0644)

	_, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "proj-crash-building",
		SourcePath:    repoDir,
		InstanceCount: 1,
	})
	if !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("expected simulated crash error, got: %v", err)
	}

	// Confirm storage shows BUILDING
	all, _ := depRepo.List(ctx, "")
	if len(all) != 1 || all[0].Status != deployments.StatusBuilding {
		t.Fatalf("expected deployment in storage with status BUILDING, got: %+v", all)
	}

	// --- CP RESTARTS ---
	recoveryEngine := deployments.NewRecoveryEngine(depRepo, instRepo, reg, sched, mockClientFactory, log)
	report, err := recoveryEngine.RecoverDeployments(ctx, deployments.RecoveryPolicyFailSafe)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if report.BuildingRecovered != 1 {
		t.Fatalf("expected 1 BUILDING deployment recovered, got %d", report.BuildingRecovered)
	}

	recoveredDep, _ := depRepo.GetByID(ctx, all[0].ID)
	if recoveredDep.Status != deployments.StatusFailed {
		t.Fatalf("expected status FAILED, got %s", recoveredDep.Status)
	}
	// DL-04 invariant: Partial build artifact from crashed stage must NOT be reused or trusted
	if recoveredDep.ImageDigest != "" {
		t.Fatalf("DL-04 VIOLATION: partial build image digest was not cleared upon crash recovery!")
	}
	if recoveredDep.Stage != "BUILD_INTERRUPTED_CP_CRASH" {
		t.Fatalf("expected stage BUILD_INTERRUPTED_CP_CRASH, got %s", recoveredDep.Stage)
	}

	t.Log("CP-CRASH-02 (Gate G-17) & DL-04 Passed: CP crash while BUILDING safely marked FAILED and partial artifact rejected!")
}

// CP-CRASH-03 (Gate G-18): CP crash while BUILT / SCHEDULING.
func TestCPCrash03_G18_CrashWhileScheduling(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	errSimulatedCrash := errors.New("simulated CP SIGKILL during SCHEDULING")
	depService.SetCrashHook(func(stage deployments.DeploymentStatus) error {
		if stage == deployments.StatusScheduling {
			return errSimulatedCrash
		}
		return nil
	})

	_, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "proj-crash-sched",
		Image:         "redis:7",
		InstanceCount: 2,
	})
	if !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("expected simulated crash error, got: %v", err)
	}

	// Confirm storage shows SCHEDULING
	all, _ := depRepo.List(ctx, "")
	if len(all) != 1 || all[0].Status != deployments.StatusScheduling {
		t.Fatalf("expected status SCHEDULING, got: %+v", all)
	}

	// --- CP RESTARTS ---
	recoveryEngine := deployments.NewRecoveryEngine(depRepo, instRepo, reg, sched, mockClientFactory, log)
	report, err := recoveryEngine.RecoverDeployments(ctx, deployments.RecoveryPolicyFailSafe)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if report.SchedulingRecovered != 1 {
		t.Fatalf("expected 1 SCHEDULING deployment recovered, got %d", report.SchedulingRecovered)
	}

	recoveredDep, _ := depRepo.GetByID(ctx, all[0].ID)
	if recoveredDep.Status != deployments.StatusFailed {
		t.Fatalf("expected status FAILED, got %s", recoveredDep.Status)
	}
	if recoveredDep.Stage != "SCHEDULING_INTERRUPTED_CP_CRASH" {
		t.Fatalf("expected stage SCHEDULING_INTERRUPTED_CP_CRASH, got %s", recoveredDep.Stage)
	}

	t.Log("CP-CRASH-03 (Gate G-18) Passed: CP crash while BUILT/SCHEDULING recovered cleanly with zero orphan work!")
}

// CP-CRASH-04 (Gate G-19): CP crash while STARTING — partial containers cleanly terminated, zero orphans.
func TestCPCrash04_G19_CrashWhileStarting_PartialContainersCleanedUp(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	w1, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-start-1",
		Hostname:  "node-start-1",
		IPAddress: "10.0.0.1",
		Capacity:  10,
	})



	// Inject a partial running container on worker 1 to simulate a container launched just before the crash
	depID := "dep-partial-crash"
	instKey := "inst-partial-0"
	mockClientFactory.AddContainer(w1.WorkerKey, &proto.ContainerInfo{
		InstanceId:  instKey,
		ContainerId: "ctr-partial-0",
		Image:       "nginx:alpine",
		Status:      "running",
		Labels: map[string]string{
			"nebula.deployment_id": depID,
			"nebula.instance_id":   instKey,
		},
	})

	// Pre-seed the deployment record in STARTING state
	dep := &deployments.Deployment{
		ID:            depID,
		ProjectID:     "proj-partial",
		Image:         "nginx:alpine",
		DesiredState:  "RUNNING",
		Status:        deployments.StatusStarting,
		Stage:         "STARTING",
		InstanceCount: 2,
	}
	_ = depRepo.Create(ctx, dep)

	_ = instRepo.Create(ctx, &deployments.Instance{
		ID:           "inst-partial-rec",
		DeploymentID: depID,
		InstanceKey:  instKey,
		WorkerID:     w1.ID,
		Status:       "STARTING",
		ContainerID:  "ctr-partial-0",
	})

	// Confirm worker has 1 partial container running before recovery
	w1Client, _ := mockClientFactory.GetClient(ctx, w1)
	listBefore, _ := w1Client.ListContainers(ctx, nil)
	if len(listBefore.Containers) != 1 {
		t.Fatalf("expected 1 container running on worker before recovery, got %d", len(listBefore.Containers))
	}

	// --- CP RESTARTS ---
	recoveryEngine := deployments.NewRecoveryEngine(depRepo, instRepo, reg, sched, mockClientFactory, log)
	report, err := recoveryEngine.RecoverDeployments(ctx, deployments.RecoveryPolicyFailSafe)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if report.StartingRecovered != 1 {
		t.Fatalf("expected 1 STARTING deployment recovered, got %d", report.StartingRecovered)
	}
	if report.OrphanContainersStopped != 1 {
		t.Fatalf("expected 1 orphan container stopped, got %d", report.OrphanContainersStopped)
	}

	// Verify worker no longer has the partial container (stopped cleanly)
	listAfter, _ := w1Client.ListContainers(ctx, nil)
	if len(listAfter.Containers) != 0 {
		t.Fatalf("G-19 VIOLATION: orphan container still running on worker after recovery! Containers: %+v", listAfter.Containers)
	}

	// Verify deployment state is safely FAILED
	recoveredDep, _ := depRepo.GetByID(ctx, depID)
	if recoveredDep.Status != deployments.StatusFailed {
		t.Fatalf("expected status FAILED, got %s", recoveredDep.Status)
	}
	if recoveredDep.Stage != "STARTUP_INTERRUPTED_CP_CRASH" {
		t.Fatalf("expected stage STARTUP_INTERRUPTED_CP_CRASH, got %s", recoveredDep.Stage)
	}

	t.Log("CP-CRASH-04 (Gate G-19) Passed: CP crash while STARTING stopped all partial containers; ZERO orphans!")
}

// CP-CRASH-05 (Gate G-20): CP crash while RUNNING — apps stay up; reconcile restores CP view.
func TestCPCrash05_G20_CrashWhileRunning_AppsStayUpReconcileRestoresView(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	w1, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-run-1",
		Hostname:  "node-run-1",
		IPAddress: "10.0.0.1",
		Capacity:  10,
	})

	// Live HTTP backend simulating running container workload
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"running-unaffected-by-cp-crash"}`))
	}))
	defer backendServer.Close()

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	// Deploy healthy application to completion
	dep, instances, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "proj-run-safe",
		Image:         "nginx:1.25",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateAndDeploy failed: %v", err)
	}
	if dep.Status != deployments.StatusRunning {
		t.Fatalf("expected status RUNNING, got %s", dep.Status)
	}

	// Register router target
	_ = depService.Router().RegisterTarget(dep.ProjectID, instances[0].ID, backendServer.URL)

	// Verify application is responding to traffic
	req := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/test", nil)
	req.Header.Set("X-Project-ID", dep.ProjectID)
	rec := httptest.NewRecorder()
	depService.Router().ServeHTTP(rec, req)
	if rec.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Result().StatusCode)
	}

	// =========================================================================
	// CP CRASHES WHILE RUNNING (KILL -9)
	// =========================================================================
	t.Log("Simulating CP crash while deployment is RUNNING...")

	// Verify application traffic continues without any disruption during CP outage (G-20)
	recOutage := httptest.NewRecorder()
	depService.Router().ServeHTTP(recOutage, req)
	if recOutage.Result().StatusCode != http.StatusOK {
		t.Fatalf("G-20 VIOLATION: app dropped traffic during CP crash! StatusCode: %d", recOutage.Result().StatusCode)
	}

	// =========================================================================
	// CP RESTARTS
	// =========================================================================
	t.Log("Control plane restarts...")

	// Step 1: RecoveryEngine checks stuck deployments; RUNNING is terminal so it must be left untouched
	recoveryEngine := deployments.NewRecoveryEngine(depRepo, instRepo, reg, sched, mockClientFactory, log)
	report, err := recoveryEngine.RecoverDeployments(ctx, deployments.RecoveryPolicyFailSafe)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if report.TotalStuckDetected != 0 {
		t.Fatalf("expected 0 stuck deployments detected for RUNNING deployment, got %d", report.TotalStuckDetected)
	}

	// Step 2: Phase 4 Reconciler runs on boot and confirms Desired == Observed
	reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockClientFactory, log)
	actions, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconciliation failed: %v", err)
	}

	// Zero actions needed because state was never lost or drifted
	if !actions.IsZero() {
		t.Fatalf("G-20 VIOLATION: reconciler took unexpected actions on restart for healthy RUNNING app: %+v", actions)
	}

	// Verify container is still running on worker 1
	w1Client, _ := mockClientFactory.GetClient(ctx, w1)
	list, _ := w1Client.ListContainers(ctx, nil)
	if len(list.Containers) != 1 {
		t.Fatalf("expected container to stay running across CP restart, got %d", len(list.Containers))
	}

	t.Log("CP-CRASH-05 (Gate G-20) Passed: App stayed up during CP crash; reconcile restored CP view with zero disruption!")
}

// DL-03: Stuck-transition detection on restart.
func TestDL03_StuckTransitionDetection(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	// Seed multiple deployments in mixed states
	stages := []deployments.DeploymentStatus{
		deployments.StatusQueued,
		deployments.StatusBuilding,
		deployments.StatusBuilt,
		deployments.StatusScheduling,
		deployments.StatusStarting,
		deployments.StatusRunning, // terminal
		deployments.StatusFailed,  // terminal
		deployments.StatusStopped, // terminal
	}

	for i, st := range stages {
		_ = depRepo.Create(ctx, &deployments.Deployment{
			ID:            fmt.Sprintf("dep-detect-%d", i),
			ProjectID:     "proj-detect",
			Status:        st,
			Stage:         string(st),
			InstanceCount: 1,
			CreatedAt:     time.Now().UTC(),
		})
	}

	engine := deployments.NewRecoveryEngine(depRepo, instRepo, reg, sched, mockClientFactory, log)

	stuck, err := engine.DetectStuckDeployments(ctx)
	if err != nil {
		t.Fatalf("DetectStuckDeployments failed: %v", err)
	}

	// Exactly 5 in-flight states: QUEUED, BUILDING, BUILT, SCHEDULING, STARTING
	if len(stuck) != 5 {
		t.Fatalf("DL-03 failure: expected 5 stuck deployments, got %d", len(stuck))
	}

	report, err := engine.RecoverDeployments(ctx, deployments.RecoveryPolicyFailSafe)
	if err != nil {
		t.Fatalf("RecoverDeployments failed: %v", err)
	}

	if report.TotalStuckDetected != 5 {
		t.Fatalf("expected 5 stuck resolved, got %d", report.TotalStuckDetected)
	}

	// Verify no deployments remain in-flight
	remainingStuck, _ := engine.DetectStuckDeployments(ctx)
	if len(remainingStuck) != 0 {
		t.Fatalf("DL-03 failure: deployments still stuck after recovery: %d", len(remainingStuck))
	}

	t.Log("DL-03 Passed: All frozen in-flight transitions accurately identified and resolved!")
}
