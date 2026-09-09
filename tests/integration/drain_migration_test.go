package integration

import (
	"context"
	"testing"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// TestDRN02_GradualDrainMigration verifies DRN-02:
// Draining a worker gradually moves existing workloads to an eligible healthy worker
// rather than force-killing them instantly.
func TestDRN02_GradualDrainMigration(t *testing.T) {
	log := zerolog.Nop()
	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	ctx := context.Background()

	// 1. Register Worker 1 (active) and Worker 2 (spare capacity)
	w1, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-drain-source",
		Hostname:  "host-source",
		IPAddress: "10.0.1.1",
		Capacity:  5,
	})
	if err != nil {
		t.Fatalf("failed to register worker 1: %v", err)
	}

	w2, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-drain-target",
		Hostname:  "host-target",
		IPAddress: "10.0.1.2",
		Capacity:  5,
	})
	if err != nil {
		t.Fatalf("failed to register worker 2: %v", err)
	}

	// 2. Create an initial deployment running on Worker 1
	depID := "dep-migration-test"
	dep := &deployments.Deployment{
		ID:            depID,
		Image:         "redis:alpine",
		Status:        deployments.StatusRunning,
		Stage:         "RUNNING",
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, dep)

	inst := &deployments.Instance{
		ID:           "inst-mig-1",
		DeploymentID: depID,
		WorkerID:     w1.ID,
		InstanceKey:  "redis-instance-0",
		Status:       "RUNNING",
		ContainerID:  "old-container-on-w1",
	}
	_ = instRepo.Create(ctx, inst)
	reg.UpdateWorkload(w1.WorkerKey, 1)

	// 3. Mark Worker 1 as DRAINING
	drainedW1, err := reg.Drain(ctx, w1.WorkerKey)
	if err != nil {
		t.Fatalf("failed to drain worker 1: %v", err)
	}
	if drainedW1.State != workers.StateDraining {
		t.Fatalf("expected state DRAINING, got %s", drainedW1.State)
	}

	// Step 4 (DRN-02 key requirement): Workload is NOT force-killed instantly!
	checkInst, _ := instRepo.GetByID(ctx, inst.ID)
	if checkInst.Status != "RUNNING" || checkInst.WorkerID != w1.ID {
		t.Fatalf("workload was unexpectedly altered before migration loop ran")
	}

	// 5. Run Reconciler to gradually migrate workloads off the draining worker
	reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockClientFactory, log)
	migratedCount, err := reconciler.ReconcileDraining(ctx)
	if err != nil {
		t.Fatalf("ReconcileDraining failed: %v", err)
	}
	if migratedCount != 1 {
		t.Fatalf("expected 1 migrated instance, got %d", migratedCount)
	}

	// 6. Verify workload is now running on Worker 2
	migratedInst, _ := instRepo.GetByID(ctx, inst.ID)
	if migratedInst.WorkerID != w2.ID {
		t.Fatalf("expected instance to be assigned to worker 2 (%s), got %s", w2.ID, migratedInst.WorkerID)
	}
	if migratedInst.ContainerID == "old-container-on-w1" {
		t.Fatalf("expected new container ID after migration, got old container ID")
	}

	// 7. Verify replacement container was launched on Worker 2 and old container was stopped on Worker 1
	if len(mockClientFactory.Dispatched) != 1 {
		t.Fatalf("expected 1 container launch on replacement worker, got %d", len(mockClientFactory.Dispatched))
	}
	if mockClientFactory.TargetWorker[inst.InstanceKey] != w2.WorkerKey {
		t.Fatalf("expected container launched on worker 2, got %s", mockClientFactory.TargetWorker[inst.InstanceKey])
	}
	if len(mockClientFactory.Stopped) != 1 {
		t.Fatalf("expected 1 container stop call on draining worker, got %d", len(mockClientFactory.Stopped))
	}

	// 8. Verify workload accounting
	w1After, _ := reg.Get(w1.WorkerKey)
	w2After, _ := reg.Get(w2.WorkerKey)
	if w1After.ActiveWorkloads != 0 {
		t.Fatalf("expected draining worker active workloads to be 0, got %d", w1After.ActiveWorkloads)
	}
	if w2After.ActiveWorkloads != 1 {
		t.Fatalf("expected target worker active workloads to be 1, got %d", w2After.ActiveWorkloads)
	}

	t.Log("DRN-02 Passed: Draining worker workloads were gradually migrated without instant force-kill!")
}
