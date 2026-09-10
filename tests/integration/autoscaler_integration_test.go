package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/autoscaler"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	proto "github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// TestAutoscaler_Integration_SchedulerReuseAndReadiness verifies:
// 1. Desired-replica delta feeds the existing Scheduler unchanged (§18).
// 2. Readiness safeguard: new instances join LB rotation only after confirmed RUNNING.
// 3. Scale-down cleans up LB targets and stops worker containers.
// 4. Autoscaler decisions emit fully explainable events to EventRepository.
func TestAutoscaler_Integration_SchedulerReuseAndReadiness(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	projectRepo := projects.NewMemoryProjectRepository()
	eventRepo := deployments.NewMemoryEventRepository()
	workerRepo := workers.NewMemoryWorkerRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	clientFactory := deployments.NewMockWorkerClientFactory()

	// Register 2 worker nodes
	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-node-1",
		Hostname:  "worker-1.internal",
		IPAddress: "10.0.1.10",
		GRPCPort:  50051,
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker 1: %v", err)
	}

	_, err = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-node-2",
		Hostname:  "worker-2.internal",
		IPAddress: "10.0.1.11",
		GRPCPort:  50051,
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker 2: %v", err)
	}

	router := loadbalancer.NewRouter(log)
	depService := deployments.NewService(depRepo, instRepo, reg, sched, clientFactory, log)
	depService.SetEventRepo(eventRepo)
	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	depService.SetBuildAndRegistry(orchestrator, depService.RegistryClient(), router)

	projectID := uuid.New().String()
	_ = projectRepo.Create(ctx, &projects.Project{
		ID:              projectID,
		Name:            "autoscaled-service",
		DesiredReplicas: 1,
		MinReplicas:     1,
		MaxReplicas:     5,
	})

	// Initial deployment of 1 replica
	dep, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "registry.nebula.local/apps/demo:sha256-initial",
		InstanceCount: 1,
		Ports: []deployments.PortSpec{
			{ContainerPort: 8080, HostPort: 18080, Protocol: "tcp"},
		},
	})
	if err != nil {
		t.Fatalf("create and deploy: %v", err)
	}

	// Verify initial deployment state
	if dep.Status != deployments.StatusRunning {
		t.Fatalf("expected deployment status RUNNING, got %s", dep.Status)
	}
	initialTargets := router.GetTargets(projectID)
	if len(initialTargets) != 1 {
		t.Fatalf("expected 1 target in load balancer router, got %d", len(initialTargets))
	}

	// Initialize Autoscaler with MemoryMetricsProvider
	metricsProvider := autoscaler.NewMemoryMetricsProvider()
	as := autoscaler.NewAutoscaler(metricsProvider, depService, projectRepo, eventRepo, log)

	as.SetPolicy(&autoscaler.ScalingPolicy{
		ProjectID:         projectID,
		MetricType:        autoscaler.MetricCPU,
		TargetValue:       60.0,
		Tolerance:         0.10,
		MinReplicas:       1,
		MaxReplicas:       5,
		ScaleUpCooldown:   0,
		ScaleDownCooldown: 0,
	})

	// 1. Trigger Scale-Up: CPU = 85% (> 60% target)
	metricsProvider.SetMetric(projectID, autoscaler.MetricCPU, 85.0)
	decision, err := as.EvaluateAndScale(ctx, projectID)
	if err != nil {
		t.Fatalf("evaluate and scale up: %v", err)
	}

	if decision.Action != autoscaler.ActionScaleUp || decision.DesiredReplicas != 2 {
		t.Fatalf("expected SCALE_UP to 2, got action=%s desired=%d reason=%s", decision.Action, decision.DesiredReplicas, decision.Reason)
	}

	// Verify router now has 2 running targets
	targetsAfterScaleUp := router.GetTargets(projectID)
	if len(targetsAfterScaleUp) != 2 {
		t.Fatalf("expected 2 targets in router after scale-up, got %d", len(targetsAfterScaleUp))
	}

	// Verify scheduler placed the new replica (spreading between nodes)
	instances, err := instRepo.ListByDeployment(ctx, dep.ID)
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances in repository, got %d", len(instances))
	}

	// Both instances must be RUNNING
	for _, inst := range instances {
		if inst.Status != "RUNNING" {
			t.Errorf("instance %s expected status RUNNING, got %s", inst.ID, inst.Status)
		}
	}

	// Verify explainable event feed (§18, Gate G-33)
	events, err := eventRepo.ListByProject(ctx, projectID)
	if err != nil {
		t.Fatalf("list project events: %v", err)
	}
	var foundScaleUpEvent bool
	for _, ev := range events {
		if ev.EventType == "SCALE_UP" {
			foundScaleUpEvent = true
			if ev.Metadata["desired_replicas"] != 2 {
				t.Errorf("expected event metadata desired_replicas=2, got %v", ev.Metadata["desired_replicas"])
			}
			if ev.Metadata["previous_replicas"] != 1 {
				t.Errorf("expected event metadata previous_replicas=1, got %v", ev.Metadata["previous_replicas"])
			}
		}
	}
	if !foundScaleUpEvent {
		t.Errorf("expected SCALE_UP event in event repository")
	}

	// 2. Trigger Scale-Down: CPU = 20% (< 60% target)
	metricsProvider.SetMetric(projectID, autoscaler.MetricCPU, 20.0)
	downDecision, err := as.EvaluateAndScale(ctx, projectID)
	if err != nil {
		t.Fatalf("evaluate and scale down: %v", err)
	}

	if downDecision.Action != autoscaler.ActionScaleDown || downDecision.DesiredReplicas != 1 {
		t.Fatalf("expected SCALE_DOWN to 1, got action=%s desired=%d reason=%s", downDecision.Action, downDecision.DesiredReplicas, downDecision.Reason)
	}

	// Verify router cleaned up excess target
	targetsAfterScaleDown := router.GetTargets(projectID)
	if len(targetsAfterScaleDown) != 1 {
		t.Fatalf("expected 1 target in router after scale-down, got %d", len(targetsAfterScaleDown))
	}

	// Verify worker client StopContainer was called for the removed instance
	instancesAfterDown, _ := instRepo.ListByDeployment(ctx, dep.ID)
	stoppedCount := 0
	runningCount := 0
	for _, inst := range instancesAfterDown {
		if inst.Status == "STOPPED" {
			stoppedCount++
		} else if inst.Status == "RUNNING" {
			runningCount++
		}
	}
	if stoppedCount != 1 || runningCount != 1 {
		t.Errorf("expected 1 STOPPED and 1 RUNNING instance, got stopped=%d running=%d", stoppedCount, runningCount)
	}
}

// TestAutoscaler_Integration_ReadinessFailureBlocksLBRotation asserts that if container launch fails,
// the instance is NOT registered in the load balancer rotation (§18 readiness guarantee).
func TestAutoscaler_Integration_ReadinessFailureBlocksLBRotation(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)

	// Mock worker client factory that fails on second run
	failingFactory := &failingWorkerClientFactory{
		mockFactory: deployments.NewMockWorkerClientFactory(),
		failNextRun: false,
	}

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "failing-worker-node",
		Hostname:  "worker-failing.internal",
		IPAddress: "10.0.1.50",
		Capacity:  10,
	})

	router := loadbalancer.NewRouter(log)
	depService := deployments.NewService(depRepo, instRepo, reg, sched, failingFactory, log)
	depService.SetBuildAndRegistry(build.NewOrchestrator(build.NewEphemeralSandbox(log), log), depService.RegistryClient(), router)

	projectID := uuid.New().String()
	dep, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "registry.nebula.local/apps/demo:v1",
		InstanceCount: 1,
		Ports: []deployments.PortSpec{
			{ContainerPort: 8080, HostPort: 18080, Protocol: "tcp"},
		},
	})
	if err != nil {
		t.Fatalf("create and deploy: %v", err)
	}

	// Check 1 target in router
	if len(router.GetTargets(projectID)) != 1 {
		t.Fatalf("expected 1 target in router, got %d", len(router.GetTargets(projectID)))
	}

	// Now configure failure for subsequent container runs
	failingFactory.failNextRun = true

	// Attempt scale-up to 2
	_, _, err = depService.ScaleDeployment(ctx, dep.ID, 2)
	if err == nil {
		t.Fatalf("expected error when worker fails RunContainer, got nil")
	}

	// Critical assertion: The failed container MUST NOT be added to load balancer router!
	targets := router.GetTargets(projectID)
	if len(targets) != 1 {
		t.Errorf("readiness guarantee violated: expected exactly 1 target in LB, got %d", len(targets))
	}
}

// TestAutoscaler_Integration_ReconcilerPrunesExcessReplicas tests that the background reconciler
// enforces the desired replica upper bound (§18).
func TestAutoscaler_Integration_ReconcilerPrunesExcessReplicas(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	clientFactory := deployments.NewMockWorkerClientFactory()

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "reconcile-worker",
		Hostname:  "reconcile.internal",
		IPAddress: "10.0.1.99",
		Capacity:  10,
	})

	router := loadbalancer.NewRouter(log)
	depService := deployments.NewService(depRepo, instRepo, reg, sched, clientFactory, log)
	depService.SetBuildAndRegistry(build.NewOrchestrator(build.NewEphemeralSandbox(log), log), depService.RegistryClient(), router)

	projectID := uuid.New().String()
	dep, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "registry.nebula.local/apps/demo:v1",
		InstanceCount: 4,
		Ports: []deployments.PortSpec{
			{ContainerPort: 8080, HostPort: 18080, Protocol: "tcp"},
		},
	})
	if err != nil {
		t.Fatalf("create and deploy: %v", err)
	}

	if len(router.GetTargets(projectID)) != 4 {
		t.Fatalf("expected 4 targets initially, got %d", len(router.GetTargets(projectID)))
	}

	// Update desired replicas down to 2 in deployment repository
	dep.InstanceCount = 2
	dep.DesiredReplicas = 2
	_ = depRepo.UpdateScale(ctx, dep.ID, 2)

	// Run Reconciler loop
	reconciler := reconcile.NewReconciler(
		reg,
		depRepo,
		instRepo,
		sched,
		clientFactory,
		log,
	)
	reconciler.SetRouter(router)

	// Trigger single reconcile tick
	actions, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile once: %v", err)
	}
	if actions.StoppedCount != 2 {
		t.Errorf("expected 2 excess instances stopped, got %d", actions.StoppedCount)
	}

	// Assert: Reconciler must prune the 2 excess replicas
	runningInstances, _ := instRepo.ListByDeployment(ctx, dep.ID)
	activeCount := 0
	for _, inst := range runningInstances {
		if inst.Status == "RUNNING" {
			activeCount++
		}
	}
	if activeCount != 2 {
		t.Errorf("reconciler failed to converge to desired replica count: expected 2 running, got %d", activeCount)
	}

	// Assert router targets also pruned to 2
	targets := router.GetTargets(projectID)
	if len(targets) != 2 {
		t.Errorf("expected 2 targets in router after reconcile, got %d", len(targets))
	}
}

type failingWorkerClientFactory struct {
	mockFactory *deployments.MockWorkerClientFactory
	failNextRun bool
}

func (f *failingWorkerClientFactory) GetClient(ctx context.Context, w *workers.Worker) (deployments.WorkerClient, error) {
	client, err := f.mockFactory.GetClient(ctx, w)
	if err != nil {
		return nil, err
	}
	return &failingWorkerClient{
		underlying:  client,
		failNextRun: &f.failNextRun,
	}, nil
}

type failingWorkerClient struct {
	underlying  deployments.WorkerClient
	failNextRun *bool
}

func (c *failingWorkerClient) RunContainer(ctx context.Context, in *proto.RunContainerRequest, opts ...grpc.CallOption) (*proto.RunContainerResponse, error) {
	if c.failNextRun != nil && *c.failNextRun {
		return &proto.RunContainerResponse{
			Status: "FAILED",
			Error:  "readiness check failed: container crashed immediately",
		}, nil
	}
	return c.underlying.RunContainer(ctx, in, opts...)
}

func (c *failingWorkerClient) StopContainer(ctx context.Context, in *proto.StopContainerRequest, opts ...grpc.CallOption) (*proto.StopContainerResponse, error) {
	return c.underlying.StopContainer(ctx, in, opts...)
}

func (c *failingWorkerClient) ListContainers(ctx context.Context, in *proto.ListContainersRequest, opts ...grpc.CallOption) (*proto.ListContainersResponse, error) {
	return c.underlying.ListContainers(ctx, in, opts...)
}
