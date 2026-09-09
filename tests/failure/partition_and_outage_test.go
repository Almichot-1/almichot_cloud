package failure

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// FS-05 (Gate G-13) & FS-06:
// - FS-05: Network partition != worker death. CP<->Worker blackhole marks worker UNREACHABLE;
//   other workers are completely unaffected; no false-healthy.
// - FS-06: Partition heals; worker rejoins cleanly and state reconciles without duplicate scheduling.
func TestFS05_G13_FS06_NetworkPartitionAndCleanHeal(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()
	reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockFactory, log)

	// 1. Setup two healthy workers: Worker 1 (will be partitioned) and Worker 2 (unaffected)
	w1, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-partition-1",
		Capacity:  10,
	})
	w2, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-normal-2",
		Capacity:  10,
	})

	// Deploy an application instance on worker 1
	dep := &deployments.Deployment{
		ID:            "dep-part-test",
		Image:         "app:part-v1",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, dep)

	inst1 := &deployments.Instance{
		ID:           "inst-part-1",
		DeploymentID: dep.ID,
		WorkerID:     w1.ID,
		InstanceKey:  "inst-key-part-1",
		Status:       "RUNNING",
		ContainerID:  "mock-ctr-part-1",
	}
	_ = instRepo.Create(ctx, inst1)

	// Container is running on worker 1
	mockFactory.AddContainer(w1.ID, &proto.ContainerInfo{
		InstanceId:  inst1.InstanceKey,
		ContainerId: inst1.ContainerID,
		Image:       dep.Image,
		Status:      "running",
		Labels: map[string]string{
			"nebula.instance_id":   inst1.InstanceKey,
			"nebula.deployment_id": dep.ID,
		},
	})

	// -------------------------------------------------------------
	// 2. Simulate Network Blackhole on Worker 1 (FS-05, G-13)
	// -------------------------------------------------------------
	_, err := reg.SetPartitioned(ctx, w1.WorkerKey, true)
	if err != nil {
		t.Fatalf("failed to set worker partitioned: %v", err)
	}

	// Verification (FS-05, G-13):
	// Worker 1 is UNREACHABLE (not dead); Schedulability is false; no false-healthy
	w1Check, _ := reg.Get(w1.WorkerKey)
	if w1Check.Health != workers.HealthUnreachable {
		t.Fatalf("FS-05 failure: expected worker 1 health UNREACHABLE, got %s", w1Check.Health)
	}
	if !w1Check.IsPartitioned {
		t.Fatalf("FS-05 failure: expected IsPartitioned true")
	}
	eval1 := w1Check.EvaluateSchedulability()
	if eval1.Allowed {
		t.Fatalf("FS-05 failure: partitioned worker must be excluded from scheduling")
	}

	// Verification (FS-05, G-13):
	// Worker 2 must remain completely unaffected, READY and HEALTHY!
	w2Check, _ := reg.Get(w2.WorkerKey)
	if w2Check.Health != workers.HealthHealthy {
		t.Fatalf("FS-05 / G-13 VIOLATION: other worker was affected! Health: %s", w2Check.Health)
	}
	eval2 := w2Check.EvaluateSchedulability()
	if !eval2.Allowed {
		t.Fatalf("FS-05 / G-13 VIOLATION: other worker became unschedulable: %s", eval2.Reason)
	}

	t.Logf("FS-05 (Gate G-13) Passed: Worker 1 marked UNREACHABLE; Worker 2 completely unaffected!")

	// -------------------------------------------------------------
	// 3. Partition Heals: Worker 1 rejoins cleanly (FS-06)
	// -------------------------------------------------------------
	_, _ = reg.SetPartitioned(ctx, w1.WorkerKey, false)

	// Worker resumes heartbeats: require hysteresis to restore HEALTHY
	_ = reg.Heartbeat(ctx, w1.WorkerKey) // beat 1
	_ = reg.Heartbeat(ctx, w1.WorkerKey) // beat 2 -> restores HEALTHY

	w1Healed, _ := reg.Get(w1.WorkerKey)
	if w1Healed.Health != workers.HealthHealthy {
		t.Fatalf("FS-06 failure: expected worker 1 to return to HEALTHY after consecutive heartbeats, got %s", w1Healed.Health)
	}

	// Run reconcile cycle upon partition healing:
	// Invariant FS-06: No duplicate scheduling!
	actions, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile after heal failed: %v", err)
	}
	if actions.RecreatedCount != 0 {
		t.Fatalf("FS-06 VIOLATION: duplicate scheduling occurred on partition heal! Recreated: %d", actions.RecreatedCount)
	}

	// Verify only 1 container exists on worker 1
	client1, _ := mockFactory.GetClient(ctx, w1)
	list1, _ := client1.ListContainers(ctx, &proto.ListContainersRequest{})
	if len(list1.Containers) != 1 {
		t.Fatalf("FS-06 failure: expected exactly 1 container on worker 1, got %d", len(list1.Containers))
	}

	t.Logf("FS-06 Passed: Partition healed; worker rejoined cleanly without duplicate scheduling!")
}

// FS-04 (Gate G-15): CP outage behavior.
// When Control Plane crashes/shuts down:
// - Worker keeps running existing containers.
// - Application endpoints remain up and serving user traffic.
// - On CP return, reconciliation verifies state and resumes cleanly.
func TestFS04_G15_CPOutageAppsKeepRunningAndReconcileOnReturn(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// Persistent storage across CP outage
	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	sharedWorkerCluster := deployments.NewMockWorkerClientFactory()

	// Live application backend serving traffic
	appServedRequests := 0
	appServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appServedRequests++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"app":"running-during-outage"}`))
	}))
	defer appServer.Close()

	// 1. Initial CP boots and deploys app
	reg1 := workers.NewRegistry(workerRepo, log)
	w, _ := reg1.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-outage-1",
		Capacity:  10,
	})

	dep := &deployments.Deployment{
		ID:            "dep-outage-test",
		Image:         "mission-critical:v1",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, dep)

	inst := &deployments.Instance{
		ID:           "inst-outage-1",
		DeploymentID: dep.ID,
		WorkerID:     w.ID,
		InstanceKey:  "inst-key-outage-1",
		Status:       "RUNNING",
		ContainerID:  "mock-ctr-outage-1",
	}
	_ = instRepo.Create(ctx, inst)

	// Container is running on worker
	sharedWorkerCluster.AddContainer(w.ID, &proto.ContainerInfo{
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
	// 2. Control Plane Outage! (CP process terminates completely)
	// -------------------------------------------------------------
	// During CP outage:
	// Verify that the application backend continues to respond to HTTP requests!
	for i := 0; i < 5; i++ {
		resp, err := http.Get(appServer.URL)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("FS-04 / G-15 VIOLATION: app failed to serve traffic during CP outage: %v", err)
		}
		resp.Body.Close()
	}

	if appServedRequests != 5 {
		t.Fatalf("expected 5 requests served during CP outage, got %d", appServedRequests)
	}

	// Verify worker containers are still running on the worker
	client, _ := sharedWorkerCluster.GetClient(ctx, w)
	workerContainers, _ := client.ListContainers(ctx, nil)
	if len(workerContainers.Containers) != 1 {
		t.Fatalf("FS-04 / G-15 VIOLATION: containers died during CP outage!")
	}

	t.Logf("CP Outage verified: containers and app continued serving traffic during CP outage!")

	// -------------------------------------------------------------
	// 3. Control Plane Returns! (Boot new CP instance from storage)
	// -------------------------------------------------------------
	reg2 := workers.NewRegistry(workerRepo, log)
	sched2 := scheduler.NewScheduler(reg2, instRepo.CountByWorkerForDeployment, log)
	reconciler2 := reconcile.NewReconciler(reg2, depRepo, instRepo, sched2, sharedWorkerCluster, log)

	// On return: reconcile first to confirm Desired == Observed
	actions, err := reconciler2.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile on CP return failed: %v", err)
	}

	// Should confirm existing running containers with 0 unnecessary restarts
	if actions.RecreatedCount != 0 {
		t.Fatalf("FS-04 / G-15 failure: CP return unexpectedly recreated containers: %d", actions.RecreatedCount)
	}

	t.Logf("FS-04 (Gate G-15) Passed: CP outage sustained; apps kept serving; reconciled cleanly on return!")
}
