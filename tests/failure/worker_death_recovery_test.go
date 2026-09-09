package failure

import (
	"context"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/discovery"
	"github.com/nebula/nebula/internal/heartbeat"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// FS-01 (Gate G-12): Worker death -> full recovery cycle:
// 1. Worker process killed -> heartbeats expire.
// 2. Heartbeat timeout monitor marks worker UNHEALTHY.
// 3. Endpoints on dead worker are disabled/evicted from routing (G-10).
// 4. Reconciler detects missing instance and scheduler places replacement on healthy worker.
// 5. Container starts, endpoint registers, Desired == Observed achieved.
func TestFS01_G12_WorkerDeath_FullRecoveryCycle(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// 1. Infrastructure setup
	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	router := loadbalancer.NewRouter(log)
	serviceReg := discovery.NewServiceRegistry(router, log)

	reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockClientFactory, log)
	reconciler.SetServiceRegistry(serviceReg)

	// Configure fast timeout monitor for test: 50ms interval, 3 misses = 150ms
	cfg := workers.HealthStateMachineConfig{
		HeartbeatInterval:      50 * time.Millisecond,
		SuspectedThreshold:     2,
		UnhealthyThreshold:     3,
		ConsecutiveBeatsToHeal: 2,
	}
	reg.SetHealthConfig(cfg)
	monitor := heartbeat.NewTimeoutMonitor(reg, serviceReg, cfg, log)

	// 2. Register two workers:
	// Worker 1: will die
	// Worker 2: healthy replacement
	w1, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-doomed-1",
		Hostname:  "node-1",
		IPAddress: "10.0.1.1",
		Capacity:  10,
	})
	w2, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-healthy-2",
		Hostname:  "node-2",
		IPAddress: "10.0.1.2",
		Capacity:  10,
	})

	projectID := "proj-death-recovery"
	dep := &deployments.Deployment{
		ID:            "dep-death-1",
		ProjectID:     projectID,
		Image:         "web-service:v1",
		Status:        deployments.StatusRunning,
		DesiredState:  "RUNNING",
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, dep)

	inst1 := &deployments.Instance{
		ID:           "inst-death-1",
		DeploymentID: dep.ID,
		WorkerID:     w1.ID,
		InstanceKey:  "inst-key-death-1",
		Status:       "RUNNING",
		ContainerID:  "mock-ctr-w1",
	}
	_ = instRepo.Create(ctx, inst1)

	// Container is running on worker 1
	mockClientFactory.AddContainer(w1.ID, &proto.ContainerInfo{
		InstanceId:  inst1.InstanceKey,
		ContainerId: inst1.ContainerID,
		Image:       dep.Image,
		Status:      "running",
		Labels: map[string]string{
			"nebula.instance_id":   inst1.InstanceKey,
			"nebula.deployment_id": dep.ID,
		},
	})

	// Register initial endpoint in Service Discovery & Router
	_ = serviceReg.RegisterEndpoint(ctx, discovery.Endpoint{
		InstanceID:   inst1.ID,
		DeploymentID: dep.ID,
		ProjectID:    projectID,
		WorkerID:     w1.ID,
		Address:      "http://10.0.1.1:8080",
		Status:       discovery.EndpointHealthy,
	})

	// Verify initially reachable via load balancer
	targetsBefore := router.GetTargets(projectID)
	if len(targetsBefore) != 1 {
		t.Fatalf("expected 1 target in router initially, got %d", len(targetsBefore))
	}

	t.Logf("Initial state: instance %s running on worker 1, routed in load balancer", inst1.InstanceKey)

	// -------------------------------------------------------------
	// 3. Worker 1 Dies! (Process killed -> heartbeats stop)
	// -------------------------------------------------------------
	// Advance time past 3 missed heartbeat intervals (e.g. 250ms)
	now := time.Now().UTC().Add(250 * time.Millisecond)

	// Worker 1 process is killed (no heartbeats sent).
	// Worker 2 remains healthy, continuing to heartbeat at 'now'.
	_ = reg.HeartbeatAt(ctx, w2.WorkerKey, now)

	transitions := monitor.CheckWorkers(ctx, now)

	if len(transitions) == 0 {
		t.Fatalf("FS-01 / G-12 failure: monitor failed to detect missed heartbeats")
	}

	w1Check, _ := reg.Get(w1.WorkerKey)
	if w1Check.Health != workers.HealthUnhealthy {
		t.Fatalf("FS-01 / G-12 failure: expected worker 1 to transition to UNHEALTHY, got %s", w1Check.Health)
	}

	// -------------------------------------------------------------
	// 4. Invariant Check (G-10): Endpoints on dead worker disabled in routing
	// -------------------------------------------------------------
	targetsAfterDeath := router.GetTargets(projectID)
	if len(targetsAfterDeath) != 0 {
		t.Fatalf("G-10 VIOLATION: dead worker's endpoint was not removed from router! Targets: %+v", targetsAfterDeath)
	}
	healthyEPs := serviceReg.GetHealthyEndpoints(projectID)
	if len(healthyEPs) != 0 {
		t.Fatalf("G-10 VIOLATION: dead worker's endpoint still listed in service discovery!")
	}

	t.Logf("Worker death detected: worker 1 marked UNHEALTHY, endpoints evicted from load balancer")

	// -------------------------------------------------------------
	// 5. Reconcile Cycle Executes: Self-healing replacement scheduled (G-12)
	// -------------------------------------------------------------
	actions, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile cycle failed: %v", err)
	}
	t.Logf("Reconcile actions: %+v, errors: %v", actions, actions.Errors)

	if actions.RecreatedCount != 1 {
		t.Fatalf("FS-01 / G-12 failure: expected 1 replacement instance recreated, got %d, errors: %v", actions.RecreatedCount, actions.Errors)
	}

	// Verify replacement instance moved to worker 2 (healthy replacement)
	updatedInst, _ := instRepo.GetByInstanceKey(ctx, inst1.InstanceKey)
	if updatedInst.WorkerID != w2.ID {
		t.Fatalf("FS-01 / G-12 failure: expected instance placed on healthy worker 2 (%s), got %s",
			w2.ID, updatedInst.WorkerID)
	}
	if updatedInst.Status != "RUNNING" {
		t.Fatalf("FS-01 / G-12 failure: expected status RUNNING, got %s", updatedInst.Status)
	}

	// Verify container is running on worker 2
	client2, _ := mockClientFactory.GetClient(ctx, w2)
	list2, _ := client2.ListContainers(ctx, nil)
	if len(list2.Containers) != 1 {
		t.Fatalf("FS-01 / G-12 failure: expected 1 container running on worker 2, got %d", len(list2.Containers))
	}
	if list2.Containers[0].InstanceId != inst1.InstanceKey {
		t.Fatalf("expected instance key %s, got %s", inst1.InstanceKey, list2.Containers[0].InstanceId)
	}

	// Verify replacement endpoint is registered in service discovery and load balancer
	endpointsRestored := router.GetTargets(projectID)
	if len(endpointsRestored) != 1 {
		t.Fatalf("expected 1 replacement target in load balancer, got %d", len(endpointsRestored))
	}

	// Reconcile again: must be converged with zero actions (idempotence)
	actions2, err := reconciler.ReconcileOnce(ctx)
	if err != nil || !actions2.IsZero() {
		t.Fatalf("expected converged state after replacement, got actions: %+v", actions2)
	}

	t.Logf("FS-01 (Gate G-12) Passed: Worker death detected -> endpoints evicted -> replacement scheduled on worker 2 -> Desired==Observed converged!")
}
