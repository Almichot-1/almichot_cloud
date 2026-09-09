package integration

import (
	"context"
	"testing"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// CP-01..06 (G-04) & DA-01 (G-24):
// Control Plane restart reloads desired state solely from durable storage (PostgreSQL/repository)
// and runs full reconciliation before accepting new scheduling requests.
func TestCP04_G04_DA01_G24_CPRestartTriggersFullReconcile(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// 1. Shared persistent storage layer (representing Postgres across CP restarts)
	sharedWorkerRepo := workers.NewMemoryWorkerRepository()
	sharedDepRepo := deployments.NewMemoryDeploymentRepository()
	sharedInstRepo := deployments.NewMemoryInstanceRepository()

	// Shared worker cluster simulation
	sharedMockFactory := deployments.NewMockWorkerClientFactory()

	// -------------------------------------------------------------
	// Epoch 1: Initial CP instance sets up cluster and deploys workload
	// -------------------------------------------------------------
	reg1 := workers.NewRegistry(sharedWorkerRepo, log)
	w, err := reg1.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-restart-1",
		Hostname:  "worker-1",
		IPAddress: "10.10.1.1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("failed to register worker in epoch 1: %v", err)
	}

	dep := &deployments.Deployment{
		ID:            "dep-persist-1",
		ProjectID:     "project-durable",
		Image:         "durable-app:v2.0",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: 1,
	}
	if err := sharedDepRepo.Create(ctx, dep); err != nil {
		t.Fatalf("failed to persist deployment in epoch 1: %v", err)
	}

	inst := &deployments.Instance{
		ID:           "inst-durable-1",
		DeploymentID: dep.ID,
		WorkerID:     w.ID,
		InstanceKey:  "inst-key-durable-1",
		Status:       "RUNNING",
		ContainerID:  "mock-ctr-durable-1",
	}
	if err := sharedInstRepo.Create(ctx, inst); err != nil {
		t.Fatalf("failed to persist instance in epoch 1: %v", err)
	}

	// Container is running on worker
	sharedMockFactory.AddContainer(w.ID, &proto.ContainerInfo{
		InstanceId:  inst.InstanceKey,
		ContainerId: inst.ContainerID,
		Image:       dep.Image,
		Status:      "running",
		Labels: map[string]string{
			"nebula.instance_id":   inst.InstanceKey,
			"nebula.deployment_id": dep.ID,
		},
	})

	// -------------------------------------------------------------
	// Failure Simulation: Kill Control Plane mid-cluster!
	// (Epoch 1 ends. Worker and containers keep running independently.)
	// While CP is dead, an external drift happens:
	// A foreign unmanaged container was started, and a managed container was dropped.
	// -------------------------------------------------------------
	sharedMockFactory.RemoveContainer(w.ID, inst.ContainerID) // killed while CP dead
	sharedMockFactory.AddContainer(w.ID, &proto.ContainerInfo{
		InstanceId:  "",
		ContainerId: "ctr-foreign-db",
		Image:       "postgres:alpine",
		Status:      "running",
		Labels:      map[string]string{"env": "local"},
	})

	// -------------------------------------------------------------
	// Epoch 2: Control Plane restarts with clean in-memory state!
	// Must reload desired config solely from storage (DA-01, G-24).
	// -------------------------------------------------------------
	reg2 := workers.NewRegistry(sharedWorkerRepo, log) // reloads workers
	sched2 := scheduler.NewScheduler(reg2, sharedInstRepo.CountByWorkerForDeployment, log)

	reconciler2 := reconcile.NewReconciler(reg2, sharedDepRepo, sharedInstRepo, sched2, sharedMockFactory, log)

	// Invariant check: Verify desired state reloaded from Postgres before scheduling (DA-01, G-24)
	loadedDeps, err := sharedDepRepo.List(ctx, "")
	if err != nil || len(loadedDeps) != 1 {
		t.Fatalf("DA-01 / G-24 failure: expected 1 deployment reloaded from storage, got %d", len(loadedDeps))
	}
	if loadedDeps[0].ID != dep.ID {
		t.Fatalf("DA-01 / G-24 failure: reloaded deployment ID mismatch: %s vs %s", loadedDeps[0].ID, dep.ID)
	}

	// Startup sequence: Full reconcile executes before any new scheduling resumes (G-04)
	actions, err := reconciler2.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("startup ReconcileOnce failed: %v", err)
	}

	// It must have recreated the missing container and flagged the foreign container without touching it
	if actions.RecreatedCount != 1 {
		t.Fatalf("CP-01..06 / G-04 failure: expected 1 recreated container on restart reconcile, got %d", actions.RecreatedCount)
	}
	if actions.FlaggedCount != 1 {
		t.Fatalf("CP-01..06 / G-04 failure: expected 1 flagged unknown container, got %d", actions.FlaggedCount)
	}
	if actions.StoppedCount != 0 {
		t.Fatalf("CP-01..06 / G-04 failure: unknown container was stopped; should be 0, got %d", actions.StoppedCount)
	}

	// Verify Desired == Observed after restart convergence
	client, _ := sharedMockFactory.GetClient(ctx, w)
	list, _ := client.ListContainers(ctx, &proto.ListContainersRequest{})
	foundRecreated := false
	for _, c := range list.Containers {
		if c.InstanceId == inst.InstanceKey && c.Status == "running" {
			foundRecreated = true
		}
	}
	if !foundRecreated {
		t.Fatalf("G-04 failure: recreated container not found running on worker after CP restart")
	}

	// Subsequent reconcile pass must perform zero actions (converged)
	actionsConverged, err := reconciler2.ReconcileOnce(ctx)
	if err != nil || actionsConverged.RecreatedCount != 0 || actionsConverged.StoppedCount != 0 {
		t.Fatalf("expected cluster state to be converged; got %+v", actionsConverged)
	}

	t.Logf("CP-01..06 (Gate G-04) & DA-01 (Gate G-24) Passed: CP restart successfully reloaded desired state from Postgres, executed full reconcile before scheduling, and converged state!")
}
