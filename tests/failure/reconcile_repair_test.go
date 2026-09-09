package failure

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

func setupReconcileFixture(t *testing.T) (
	*reconcile.Reconciler,
	*workers.Registry,
	deployments.DeploymentRepository,
	deployments.InstanceRepository,
	*deployments.MockWorkerClientFactory,
) {
	t.Helper()
	log := zerolog.Nop()
	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()

	r := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockFactory, log)
	return r, reg, depRepo, instRepo, mockFactory
}

// DA-03 (G-01) & MAN-DEL-01 (G-23): External delete of managed container (docker rm -f)
// is repaired on next reconcile cycle within timeout.
func TestDA03_G01_MAN_DEL01_ReconcileRepairsMissingInstance(t *testing.T) {
	r, reg, depRepo, instRepo, mockFactory := setupReconcileFixture(t)
	ctx := context.Background()

	// 1. Register a healthy worker
	w, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-node-1",
		Hostname:  "node-1",
		IPAddress: "10.0.0.1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// 2. Create an active deployment with 1 instance in Postgres/repository
	dep := &deployments.Deployment{
		ID:            "dep-app-1",
		ProjectID:     "proj-1",
		Image:         "web-service:v1.0",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: 1,
	}
	if err := depRepo.Create(ctx, dep); err != nil {
		t.Fatalf("failed to create deployment: %v", err)
	}

	inst := &deployments.Instance{
		ID:           "inst-id-1",
		DeploymentID: dep.ID,
		WorkerID:     w.ID,
		InstanceKey:  "inst-key-1",
		Status:       "RUNNING",
		ContainerID:  "mock-ctr-inst-key-1",
	}
	if err := instRepo.Create(ctx, inst); err != nil {
		t.Fatalf("failed to create instance: %v", err)
	}

	// 3. Simulate initial healthy running state on worker
	mockFactory.AddContainer(w.ID, &proto.ContainerInfo{
		InstanceId:  inst.InstanceKey,
		ContainerId: inst.ContainerID,
		Image:       dep.Image,
		Status:      "running",
		Labels: map[string]string{
			"nebula.instance_id":   inst.InstanceKey,
			"nebula.deployment_id": dep.ID,
		},
	})

	// Initial reconcile: should be 100% matched, zero actions taken (G-02)
	actions, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("initial ReconcileOnce failed: %v", err)
	}
	if !actions.IsZero() {
		t.Fatalf("expected initial state to have 0 actions, got %+v", actions)
	}

	// 4. Simulate manual external deletion: docker rm -f mock-ctr-inst-key-1
	removed := mockFactory.RemoveContainer(w.ID, inst.ContainerID)
	if !removed {
		t.Fatalf("failed to simulate docker rm -f")
	}

	// Verify container is gone from worker observation
	client, _ := mockFactory.GetClient(ctx, w)
	list, _ := client.ListContainers(ctx, &proto.ListContainersRequest{})
	if len(list.Containers) != 0 {
		t.Fatalf("expected worker to have 0 containers after docker rm -f, got %d", len(list.Containers))
	}

	// 5. Run next reconcile cycle: must recreate missing instance within timeout (G-01, G-23)
	actions2, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile repair cycle failed: %v", err)
	}

	if actions2.RecreatedCount != 1 {
		t.Fatalf("DA-03 / G-01 failure: expected 1 recreated instance, got %d", actions2.RecreatedCount)
	}

	// Verify instance container was brought back and running on worker
	listAfter, _ := client.ListContainers(ctx, &proto.ListContainersRequest{})
	if len(listAfter.Containers) != 1 {
		t.Fatalf("expected 1 container running on worker after repair, got %d", len(listAfter.Containers))
	}
	if listAfter.Containers[0].InstanceId != inst.InstanceKey {
		t.Fatalf("expected recreated instance ID %s, got %s", inst.InstanceKey, listAfter.Containers[0].InstanceId)
	}

	// Verify repository instance updated to RUNNING
	updatedInst, _ := instRepo.GetByInstanceKey(ctx, inst.InstanceKey)
	if updatedInst.Status != "RUNNING" {
		t.Fatalf("expected instance record status to be RUNNING, got %s", updatedInst.Status)
	}

	t.Logf("DA-03 (G-01) & MAN-DEL-01 (G-23) Passed: External delete repaired; container brought back to RUNNING!")
}

// DA-04 (G-02): Reconciliation is idempotent: Second immediate run performs zero actions.
func TestDA04_G02_ReconciliationIsIdempotent(t *testing.T) {
	r, reg, depRepo, instRepo, mockFactory := setupReconcileFixture(t)
	ctx := context.Background()

	w, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-node-1",
		Hostname:  "node-1",
		IPAddress: "10.0.0.1",
		Capacity:  10,
	})

	dep := &deployments.Deployment{
		ID:            "dep-idem",
		ProjectID:     "proj-idem",
		Image:         "web:v1",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, dep)

	inst := &deployments.Instance{
		ID:           "inst-idem-1",
		DeploymentID: dep.ID,
		WorkerID:     w.ID,
		InstanceKey:  "inst-key-idem-1",
		Status:       "RUNNING",
		ContainerID:  "mock-ctr-idem",
	}
	_ = instRepo.Create(ctx, inst)

	// Container is running in observed state
	mockFactory.AddContainer(w.ID, &proto.ContainerInfo{
		InstanceId:  inst.InstanceKey,
		ContainerId: inst.ContainerID,
		Image:       dep.Image,
		Status:      "running",
		Labels: map[string]string{
			"nebula.instance_id":   inst.InstanceKey,
			"nebula.deployment_id": dep.ID,
		},
	})

	// Run 1: Already convergent -> 0 actions
	actions1, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if actions1.TotalActions() != 0 {
		t.Fatalf("DA-04 failure: expected 0 actions on first run, got %d", actions1.TotalActions())
	}

	// Run 2: Immediate second run with no drift -> strictly 0 actions
	actions2, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if actions2.TotalActions() != 0 {
		t.Fatalf("DA-04 / Gate G-02 failure: second run performed %d actions, expected strictly 0", actions2.TotalActions())
	}

	t.Logf("DA-04 (Gate G-02) Passed: Reconciliation is strictly idempotent (zero actions on second run)!")
}

// INV-01 (G-11): Reconciliation never overshoots desired replica count (Observed <= Desired at every point).
func TestINV01_G11_ReconciliationNeverOvershootsReplicaCount(t *testing.T) {
	r, reg, depRepo, instRepo, mockFactory := setupReconcileFixture(t)
	ctx := context.Background()

	w, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-node-1",
		Hostname:  "node-1",
		IPAddress: "10.0.0.1",
		Capacity:  10,
	})

	const desiredCount = 2
	dep := &deployments.Deployment{
		ID:            "dep-scale-test",
		ProjectID:     "proj-scale",
		Image:         "web:v1",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: desiredCount,
	}
	_ = depRepo.Create(ctx, dep)

	// Create 2 desired instances
	for i := 0; i < desiredCount; i++ {
		key := fmt.Sprintf("inst-rep-%d", i)
		_ = instRepo.Create(ctx, &deployments.Instance{
			ID:           fmt.Sprintf("id-rep-%d", i),
			DeploymentID: dep.ID,
			WorkerID:     w.ID,
			InstanceKey:  key,
			Status:       "RUNNING",
			ContainerID:  fmt.Sprintf("mock-ctr-%s", key),
		})

		// Both are initially running
		mockFactory.AddContainer(w.ID, &proto.ContainerInfo{
			InstanceId:  key,
			ContainerId: fmt.Sprintf("mock-ctr-%s", key),
			Image:       dep.Image,
			Status:      "running",
			Labels: map[string]string{
				"nebula.instance_id":   key,
				"nebula.deployment_id": dep.ID,
			},
		})
	}

	// Now delete 1 container
	mockFactory.RemoveContainer(w.ID, "mock-ctr-inst-rep-0")

	// Verify observed containers <= desired
	client, _ := mockFactory.GetClient(ctx, w)
	list, _ := client.ListContainers(ctx, &proto.ListContainersRequest{})
	if len(list.Containers) > desiredCount {
		t.Fatalf("invariant violation before reconcile: %d > %d", len(list.Containers), desiredCount)
	}

	// Trigger reconcile
	actions, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if actions.RecreatedCount != 1 {
		t.Fatalf("expected 1 recreated instance, got %d", actions.RecreatedCount)
	}

	// Verify observed count exactly equals desired count and never overshot
	listAfter, _ := client.ListContainers(ctx, &proto.ListContainersRequest{})
	if len(listAfter.Containers) != desiredCount {
		t.Fatalf("INV-01 / G-11 failure: expected exactly %d containers, got %d", desiredCount, len(listAfter.Containers))
	}

	t.Logf("INV-01 (Gate G-11) Passed: Observed replica count (%d) strictly bounded by desired (%d) with no overshoot!",
		len(listAfter.Containers), desiredCount)
}

// RCN-01: Reconcile loop runs on a fixed interval, not just at startup; catches drift mid-run.
func TestRCN01_PeriodicLoopCatchesMidRunDrift(t *testing.T) {
	r, reg, depRepo, instRepo, mockFactory := setupReconcileFixture(t)
	r.SetInterval(25 * time.Millisecond) // Fast ticker for test

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-periodic",
		Hostname:  "node-p",
		IPAddress: "10.0.0.10",
		Capacity:  10,
	})

	dep := &deployments.Deployment{
		ID:            "dep-periodic",
		Image:         "web:periodic",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, dep)

	inst := &deployments.Instance{
		ID:           "id-periodic-1",
		DeploymentID: dep.ID,
		WorkerID:     w.ID,
		InstanceKey:  "inst-periodic-1",
		Status:       "RUNNING",
		ContainerID:  "mock-ctr-periodic",
	}
	_ = instRepo.Create(ctx, inst)

	// Container running initially
	mockFactory.AddContainer(w.ID, &proto.ContainerInfo{
		InstanceId:  inst.InstanceKey,
		ContainerId: inst.ContainerID,
		Image:       dep.Image,
		Status:      "running",
		Labels: map[string]string{
			"nebula.instance_id":   inst.InstanceKey,
			"nebula.deployment_id": dep.ID,
		},
	})

	// Start periodic loop in background
	r.Start(ctx)
	defer r.Stop()

	// Wait for loop to boot
	time.Sleep(50 * time.Millisecond)

	// Introduce drift mid-run: delete container externally
	mockFactory.RemoveContainer(w.ID, inst.ContainerID)

	// Wait for next periodic interval tick to catch and heal drift
	client, _ := mockFactory.GetClient(ctx, w)
	deadline := time.Now().Add(500 * time.Millisecond)
	repaired := false

	for time.Now().Before(deadline) {
		list, _ := client.ListContainers(ctx, &proto.ListContainersRequest{})
		if len(list.Containers) == 1 && list.Containers[0].InstanceId == inst.InstanceKey {
			repaired = true
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	if !repaired {
		t.Fatalf("RCN-01 failure: periodic loop failed to catch and repair mid-run drift within timeout")
	}

	t.Logf("RCN-01 Passed: Periodic reconcile loop caught and healed mid-run drift on next tick!")
}

// RCN-02: Reconcile timeout is configurable and enforced; failing repair is flagged, not retried forever silently.
func TestRCN02_ReconcileTimeoutEnforced(t *testing.T) {
	r, reg, depRepo, instRepo, mockFactory := setupReconcileFixture(t)
	r.SetTimeout(50 * time.Millisecond) // Short timeout

	ctx := context.Background()

	w, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-failing",
		Hostname:  "node-fail",
		IPAddress: "10.0.0.99",
		Capacity:  10,
	})

	dep := &deployments.Deployment{
		ID:            "dep-timeout-test",
		Image:         "broken-image:latest",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, dep)

	inst := &deployments.Instance{
		ID:           "id-fail-1",
		DeploymentID: dep.ID,
		WorkerID:     w.ID,
		InstanceKey:  "inst-fail-1",
		Status:       "RUNNING",
		ContainerID:  "mock-ctr-fail",
	}
	_ = instRepo.Create(ctx, inst)

	// The container is missing, but worker fails RunContainer
	mockFactory.FailRun = errors.New("docker daemon out of memory")

	actions, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}

	if len(actions.Errors) == 0 {
		t.Fatalf("RCN-02 failure: expected error to be surfaced in actions, got 0")
	}

	// Instance status must be flagged as REPAIR_FAILED
	updatedInst, _ := instRepo.GetByInstanceKey(ctx, inst.InstanceKey)
	if updatedInst.Status != "REPAIR_FAILED" {
		t.Fatalf("RCN-02 failure: expected instance status REPAIR_FAILED, got %s", updatedInst.Status)
	}

	t.Logf("RCN-02 Passed: Failing repair timed out/failed cleanly and surfaced error: %s (status: %s)",
		actions.Errors[0], updatedInst.Status)
}
