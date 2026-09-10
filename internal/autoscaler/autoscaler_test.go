package autoscaler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/autoscaler"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

func setupAutoscalerTest(t *testing.T) (*autoscaler.Autoscaler, *autoscaler.MemoryMetricsProvider, *deployments.Service, *projects.MemoryProjectRepository, *workers.Registry) {
	t.Helper()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	projectRepo := projects.NewMemoryProjectRepository()
	eventRepo := deployments.NewMemoryEventRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	clientFactory := deployments.NewMockWorkerClientFactory()
	router := loadbalancer.NewRouter(log)

	svc := deployments.NewService(depRepo, instRepo, reg, sched, clientFactory, log)
	svc.SetBuildAndRegistry(nil, nil, router)
	svc.SetEventRepo(eventRepo)

	metrics := autoscaler.NewMemoryMetricsProvider()
	scaler := autoscaler.NewAutoscaler(metrics, svc, projectRepo, eventRepo, log)

	return scaler, metrics, svc, projectRepo, reg
}

// 1. Unit: Desired-replica math against fake metrics feed (threshold crossed up, down, boundary)
func TestAutoscaler_ThresholdMath(t *testing.T) {
	// (a) Boundary: within tolerance deadband (10%)
	target := 50.0
	tolerance := 0.10
	minR := 1
	maxR := 10

	// Exactly at target
	desired, action, _ := autoscaler.CalculateDesiredReplicas(3, 50.0, target, minR, maxR, tolerance)
	if action != autoscaler.ActionNone || desired != 3 {
		t.Errorf("expected ActionNone and desired=3 at target, got %s, %d", action, desired)
	}

	// Within +10% deadband (54.0 <= 55.0)
	desired, action, _ = autoscaler.CalculateDesiredReplicas(3, 54.0, target, minR, maxR, tolerance)
	if action != autoscaler.ActionNone || desired != 3 {
		t.Errorf("expected ActionNone within deadband, got %s, %d", action, desired)
	}

	// Within -10% deadband (46.0 >= 45.0)
	desired, action, _ = autoscaler.CalculateDesiredReplicas(3, 46.0, target, minR, maxR, tolerance)
	if action != autoscaler.ActionNone || desired != 3 {
		t.Errorf("expected ActionNone within deadband, got %s, %d", action, desired)
	}

	// (b) Threshold crossed up: 80.0 > 55.0 -> should scale up
	desired, action, _ = autoscaler.CalculateDesiredReplicas(2, 80.0, target, minR, maxR, tolerance)
	if action != autoscaler.ActionScaleUp {
		t.Errorf("expected ActionScaleUp for 80.0 vs target 50.0, got %s", action)
	}
	if desired <= 2 {
		t.Errorf("expected desired > 2 for scale up, got %d", desired)
	}

	// (c) Threshold crossed down: 20.0 < 45.0 -> should scale down
	desired, action, _ = autoscaler.CalculateDesiredReplicas(4, 20.0, target, minR, maxR, tolerance)
	if action != autoscaler.ActionScaleDown {
		t.Errorf("expected ActionScaleDown for 20.0 vs target 50.0, got %s", action)
	}
	if desired >= 4 {
		t.Errorf("expected desired < 4 for scale down, got %d", desired)
	}
}

// 2. Unit: Cooldown timer logic: rapid oscillating metrics within the cooldown window produce zero additional actions (§18.2)
func TestAutoscaler_CooldownSuppression(t *testing.T) {
	ctx := context.Background()
	scaler, metrics, svc, projectRepo, reg := setupAutoscalerTest(t)

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-cd-1",
		Capacity:  10,
	})

	projectID := "proj-cooldown-test"
	_ = projectRepo.Create(ctx, &projects.Project{
		ID:   projectID,
		Name: "Cooldown App",
	})

	_, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "app:v1",
		InstanceCount: 2,
	})
	if err != nil {
		t.Fatalf("deploy initial: %v", err)
	}

	policy := &autoscaler.ScalingPolicy{
		ProjectID:         projectID,
		MetricType:        autoscaler.MetricCPU,
		TargetValue:       50.0,
		Tolerance:         0.10,
		MinReplicas:       1,
		MaxReplicas:       10,
		ScaleUpCooldown:   60 * time.Second,
		ScaleDownCooldown: 180 * time.Second,
	}
	scaler.SetPolicy(policy)

	// Step 1: Initial scale up from 2 to 4
	metrics.SetMetric(projectID, autoscaler.MetricCPU, 95.0)
	dec1, err := scaler.EvaluateAndScale(ctx, projectID)
	if err != nil {
		t.Fatalf("scale up: %v", err)
	}
	if dec1.Action != autoscaler.ActionScaleUp {
		t.Fatalf("expected ActionScaleUp, got %s", dec1.Action)
	}

	// Step 2: Immediate oscillation 5 seconds later with low metric (10.0 CPU)
	// Must produce ZERO scaling actions because we are inside cooldown window
	metrics.SetMetric(projectID, autoscaler.MetricCPU, 10.0)
	dec2, err := scaler.EvaluateAndScale(ctx, projectID)
	if err != nil {
		t.Fatalf("oscillating scale check: %v", err)
	}
	if dec2.Action != autoscaler.ActionNone {
		t.Fatalf("expected ActionNone due to cooldown suppression, got %s (%s)", dec2.Action, dec2.Reason)
	}

	// Step 3: Rapid oscillation with high metric (99.0 CPU) within scale-up cooldown
	metrics.SetMetric(projectID, autoscaler.MetricCPU, 99.0)
	dec3, err := scaler.EvaluateAndScale(ctx, projectID)
	if err != nil {
		t.Fatalf("oscillating high check: %v", err)
	}
	if dec3.Action != autoscaler.ActionNone {
		t.Fatalf("expected ActionNone due to scale-up cooldown suppression, got %s (%s)", dec3.Action, dec3.Reason)
	}

	// Step 4: Advance time beyond cooldown window -> scale down is now permitted
	scaler.SetLastScaleTimes(projectID, time.Now().Add(-200*time.Second), time.Now().Add(-200*time.Second))
	metrics.SetMetric(projectID, autoscaler.MetricCPU, 10.0)
	dec4, err := scaler.EvaluateAndScale(ctx, projectID)
	if err != nil {
		t.Fatalf("scale after cooldown: %v", err)
	}
	if dec4.Action != autoscaler.ActionScaleDown {
		t.Fatalf("expected ActionScaleDown after cooldown expired, got %s (%s)", dec4.Action, dec4.Reason)
	}
}

// 3. Unit: Minimum-replica floor: scale-down math never proposes a count below configured minimum (§18.2)
func TestAutoscaler_MinReplicaFloor(t *testing.T) {
	minFloor := 3
	maxCeiling := 10
	target := 60.0

	// Extreme low load (0 CPU) on a fleet of 5 replicas
	desired, action, _ := autoscaler.CalculateDesiredReplicas(5, 0.0, target, minFloor, maxCeiling, 0.10)
	if desired < minFloor {
		t.Fatalf("expected desired replica count >= minFloor (%d), got %d", minFloor, desired)
	}
	if desired != minFloor {
		t.Errorf("expected desired to clamp exactly at floor %d, got %d", minFloor, desired)
	}
	if action != autoscaler.ActionScaleDown {
		t.Errorf("expected ActionScaleDown, got %s", action)
	}

	// Already at floor: should produce ActionNone
	desiredAtFloor, actionAtFloor, _ := autoscaler.CalculateDesiredReplicas(3, 5.0, target, minFloor, maxCeiling, 0.10)
	if desiredAtFloor != minFloor {
		t.Fatalf("expected count to remain at floor %d, got %d", minFloor, desiredAtFloor)
	}
	if actionAtFloor != autoscaler.ActionNone {
		t.Errorf("expected ActionNone when already at floor, got %s", actionAtFloor)
	}
}

// 4. Failure-mode: Metrics feed briefly unavailable -> autoscaler fails safe (holds last known desired count)
func TestAutoscaler_MetricsUnavailableFailSafe(t *testing.T) {
	ctx := context.Background()
	scaler, metrics, svc, projectRepo, reg := setupAutoscalerTest(t)

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-fs-1",
		Capacity:  10,
	})

	projectID := "proj-failsafe-test"
	_ = projectRepo.Create(ctx, &projects.Project{
		ID:   projectID,
		Name: "FailSafe App",
	})

	_, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "app:v1",
		InstanceCount: 4,
	})
	if err != nil {
		t.Fatalf("deploy initial: %v", err)
	}

	policy := &autoscaler.ScalingPolicy{
		ProjectID:   projectID,
		MetricType:  autoscaler.MetricCPU,
		TargetValue: 50.0,
		MinReplicas: 1,
		MaxReplicas: 10,
	}
	scaler.SetPolicy(policy)

	// Simulate outage on metrics provider
	metrics.SetError(errors.New("connection to prometheus/metrics-agent timed out"))

	decision, err := scaler.EvaluateAndScale(ctx, projectID)
	if err != nil {
		t.Fatalf("unexpected error from EvaluateAndScale: %v", err)
	}

	// Fail safe assertion: holds current count (4), produces ActionNone, never drops or spikes
	if decision.Action != autoscaler.ActionNone {
		t.Errorf("expected fail-safe ActionNone when metrics are down, got %s", decision.Action)
	}
	if decision.DesiredReplicas != 4 {
		t.Errorf("expected fail-safe to hold current replica count 4, got %d", decision.DesiredReplicas)
	}

	// Verify deployment state in DB was untouched
	deps, _ := svc.ListDeployments(ctx, projectID)
	if len(deps) == 0 || deps[0].InstanceCount != 4 {
		t.Errorf("expected deployment count in DB to remain 4, got %v", deps)
	}
}
