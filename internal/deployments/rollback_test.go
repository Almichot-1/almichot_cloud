package deployments_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

func setupTestService(t *testing.T) (*deployments.Service, *workers.Registry, *deployments.MockWorkerClientFactory, *loadbalancer.Router) {
	t.Helper()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	clientFactory := deployments.NewMockWorkerClientFactory()

	svc := deployments.NewService(depRepo, instRepo, reg, sched, clientFactory, log)
	router := loadbalancer.NewRouter(log)
	svc.SetBuildAndRegistry(nil, nil, router)

	return svc, reg, clientFactory, router
}

func TestRollback_SelectsPreviousKnownGoodReleaseByDigest(t *testing.T) {
	ctx := context.Background()
	svc, reg, clientFactory, router := setupTestService(t)

	// Register healthy worker
	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-1",
		Hostname:  "node-1",
		IPAddress: "127.0.0.1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	projectID := "proj-rollback-test"

	// 1. Initial Release v1
	digestV1 := "sha256:11111111111111111111111111111111"
	depV1, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "app:v1",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("create dep v1: %v", err)
	}
	// Manually attach release record for v1
	_ = svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV1.ID,
		Version:      "v1",
		ImageRef:     "app:v1",
		ImageDigest:  digestV1,
		CreatedAt:    time.Now().Add(-10 * time.Minute),
	})

	// Stop v1 as part of replacement by v2
	if _, err := svc.StopDeployment(ctx, depV1.ID); err != nil {
		t.Fatalf("stop v1: %v", err)
	}

	// 2. Second Release v2 (broken)
	digestV2 := "sha256:22222222222222222222222222222222"
	depV2, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "app:v2",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("create dep v2: %v", err)
	}
	_ = svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV2.ID,
		Version:      "v2",
		ImageRef:     "app:v2",
		ImageDigest:  digestV2,
		CreatedAt:    time.Now(),
	})

	// 3. Rollback v2 -> should restore v1 by digestV1
	newDep, instances, err := svc.Rollback(ctx, depV2.ID)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	if newDep.ImageDigest != digestV1 {
		t.Errorf("expected restored deployment to use digest %s, got %s", digestV1, newDep.ImageDigest)
	}
	if newDep.Status != deployments.StatusRunning {
		t.Errorf("expected restored deployment to be RUNNING, got %s", newDep.Status)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 running instance, got %d", len(instances))
	}

	// Verify old deployment is ROLLED_BACK
	oldDep, _, err := svc.GetDeployment(ctx, depV2.ID)
	if err != nil {
		t.Fatalf("get old deployment: %v", err)
	}
	if oldDep.Status != deployments.StatusRolledBack {
		t.Errorf("expected old deployment status ROLLED_BACK, got %s", oldDep.Status)
	}

	// Verify old instance was stopped
	if len(clientFactory.Stopped) == 0 {
		t.Errorf("expected old container to be stopped via WorkerClient")
	}

	// Verify rollback event recorded
	events, err := svc.EventRepo().ListByProject(ctx, projectID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected rollback event to be recorded in EventRepo")
	}
	foundRollbackEvent := false
	for _, ev := range events {
		if ev.EventType == "DEPLOYMENT_ROLLED_BACK" {
			foundRollbackEvent = true
			if ev.DeploymentID != depV2.ID {
				t.Errorf("expected event deployment_id %s, got %s", depV2.ID, ev.DeploymentID)
			}
		}
	}
	if !foundRollbackEvent {
		t.Errorf("expected DEPLOYMENT_ROLLED_BACK event")
	}

	// Verify load balancer routes to the restored instance
	targets := router.GetTargets(projectID)
	if len(targets) != 1 {
		t.Errorf("expected 1 active load balancer target, got %d", len(targets))
	}
}

func TestRollback_FailsWhenNoPreviousRelease(t *testing.T) {
	ctx := context.Background()
	svc, reg, _, _ := setupTestService(t)

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-1",
		Capacity:  10,
	})

	projectID := "proj-single-release"
	dep, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID: projectID,
		Image:     "app:v1",
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	// Attempt rollback when no previous release exists
	_, _, err = svc.Rollback(ctx, dep.ID)
	if err == nil {
		t.Fatalf("expected error when no previous release exists, got nil")
	}
}

func TestStopDeployment_TerminalStateAndStopsContainers(t *testing.T) {
	ctx := context.Background()
	svc, reg, clientFactory, router := setupTestService(t)

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-1",
		Capacity:  10,
	})

	dep, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID: "proj-stop-test",
		Image:     "app:v1",
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	stoppedDep, err := svc.StopDeployment(ctx, dep.ID)
	if err != nil {
		t.Fatalf("StopDeployment failed: %v", err)
	}

	if stoppedDep.Status != deployments.StatusStopped {
		t.Errorf("expected status STOPPED, got %s", stoppedDep.Status)
	}
	if !stoppedDep.Status.IsTerminal() {
		t.Errorf("expected StatusStopped to be terminal")
	}

	// Verify instances stopped
	if len(clientFactory.Stopped) == 0 {
		t.Errorf("expected StopContainer RPC to be issued")
	}

	// Verify target removed from router
	targets := router.GetTargets("proj-stop-test")
	if len(targets) != 0 {
		t.Errorf("expected 0 targets in router after stop, got %d", len(targets))
	}
}

// Golden test: rollback correctly skips a failed immediately-prior release rather than naively picking N-1 (§27.2).
func TestRollback_SkipsFailedIntermediateRelease(t *testing.T) {
	ctx := context.Background()
	svc, reg, _, _ := setupTestService(t)

	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-1",
		Hostname:  "node-1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	projectID := "proj-golden-skip"

	// 1. Release v1 (known-good)
	digestV1 := "sha256:11111111111111111111111111111111"
	depV1, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID: projectID,
		Image:     "app:v1",
	})
	if err != nil {
		t.Fatalf("deploy v1: %v", err)
	}
	_ = svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV1.ID,
		Version:      "v1",
		ImageRef:     "app:v1",
		ImageDigest:  digestV1,
		CreatedAt:    time.Now().Add(-20 * time.Minute),
	})

	// 2. Release v2 (failed deployment)
	digestV2 := "sha256:22222222222222222222222222222222"
	depV2ID := uuid.New().String()
	failedDep := &deployments.Deployment{
		ID:           depV2ID,
		ProjectID:    projectID,
		Image:        "app:v2",
		Status:       deployments.StatusFailed,
		Stage:        "FAILED",
		DesiredState: "FAILED",
		CreatedAt:    time.Now().Add(-10 * time.Minute),
	}
	if err := svc.DepRepo().Create(ctx, failedDep); err != nil {
		t.Fatalf("create failedDep: %v", err)
	}
	_ = svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV2ID,
		Version:      "v2",
		ImageRef:     "app:v2",
		ImageDigest:  digestV2,
		CreatedAt:    time.Now().Add(-10 * time.Minute),
	})

	// 3. Release v3 (current running deployment)
	digestV3 := "sha256:33333333333333333333333333333333"
	depV3, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID: projectID,
		Image:     "app:v3",
	})
	if err != nil {
		t.Fatalf("deploy v3: %v", err)
	}
	_ = svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV3.ID,
		Version:      "v3",
		ImageRef:     "app:v3",
		ImageDigest:  digestV3,
		CreatedAt:    time.Now().Add(-1 * time.Minute),
	})

	// 4. Trigger Rollback on v3
	restoredDep, instances, err := svc.Rollback(ctx, depV3.ID)
	if err != nil {
		t.Fatalf("rollback v3 failed: %v", err)
	}

	// G-30 & §27.2: Must skip v2 (which failed) and pick v1
	if restoredDep.ImageDigest != digestV1 {
		t.Fatalf("expected rollback target to be v1 with digest %s, got %s (did it naively pick failed v2?)", digestV1, restoredDep.ImageDigest)
	}
	if restoredDep.Revision != "v1" {
		t.Errorf("expected revision v1, got %s", restoredDep.Revision)
	}
	if restoredDep.Status != deployments.StatusRunning {
		t.Errorf("expected restored deployment to be RUNNING, got %s", restoredDep.Status)
	}
	if len(instances) == 0 {
		t.Errorf("expected running instances for restored deployment")
	}

	// Verify old deployment v3 is marked ROLLED_BACK
	oldDep, _, err := svc.GetDeployment(ctx, depV3.ID)
	if err != nil {
		t.Fatalf("get old dep: %v", err)
	}
	if oldDep.Status != deployments.StatusRolledBack {
		t.Errorf("expected old dep status ROLLED_BACK, got %s", oldDep.Status)
	}
}

// Partial failure test: target fails to schedule -> cleanly marked FAILED with SCHEDULING_FAILED,
// no ambiguous state, and original deployment is NOT marked ROLLED_BACK (§27.2).
func TestRollback_PartialFailure_Reporting(t *testing.T) {
	ctx := context.Background()
	svc, reg, _, _ := setupTestService(t)

	// Register worker with capacity for deployments
	worker, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-1",
		Hostname:  "node-1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	projectID := "proj-partial-fail"

	// 1. Initial Release v1
	digestV1 := "sha256:11111111111111111111111111111111"
	depV1, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID: projectID,
		Image:     "app:v1",
	})
	if err != nil {
		t.Fatalf("deploy v1: %v", err)
	}
	_ = svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV1.ID,
		Version:      "v1",
		ImageRef:     "app:v1",
		ImageDigest:  digestV1,
		CreatedAt:    time.Now().Add(-10 * time.Minute),
	})

	// 2. Deploy v2
	digestV2 := "sha256:22222222222222222222222222222222"
	depV2, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID: projectID,
		Image:     "app:v2",
	})
	if err != nil {
		t.Fatalf("deploy v2: %v", err)
	}
	_ = svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV2.ID,
		Version:      "v2",
		ImageRef:     "app:v2",
		ImageDigest:  digestV2,
		CreatedAt:    time.Now(),
	})

	// Drain worker to exhaust capacity and force scheduler failure during rollback
	_, _ = reg.Drain(ctx, worker.ID)

	// 3. Trigger Rollback on v2
	failedRollbackDep, instances, err := svc.Rollback(ctx, depV2.ID)
	if err == nil {
		t.Fatalf("expected rollback to fail due to no capacity/workers, but it succeeded")
	}
	if len(instances) != 0 {
		t.Errorf("expected 0 running instances, got %d", len(instances))
	}

	// Target deployment must be cleanly marked FAILED with stage SCHEDULING_FAILED
	if failedRollbackDep == nil {
		t.Fatalf("expected failed rollback deployment record to be returned")
	}
	if failedRollbackDep.Status != deployments.StatusFailed {
		t.Errorf("expected rollback deployment status FAILED, got %s", failedRollbackDep.Status)
	}
	if failedRollbackDep.Stage != "SCHEDULING_FAILED" {
		t.Errorf("expected rollback deployment stage SCHEDULING_FAILED, got %s", failedRollbackDep.Stage)
	}

	// Verify original deployment v2 was NOT marked ROLLED_BACK (prevent ambiguous state)
	origDep, _, err := svc.GetDeployment(ctx, depV2.ID)
	if err != nil {
		t.Fatalf("get original dep: %v", err)
	}
	if origDep.Status == deployments.StatusRolledBack {
		t.Errorf("expected original deployment status NOT to be ROLLED_BACK when rollback scheduling fails")
	}
}

