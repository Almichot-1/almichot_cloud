package autoscaler_test

import (
	"context"
	"sync"
	"testing"

	"github.com/nebula/nebula/internal/autoscaler"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/workers"
)

// TestAutoscaler_ConcurrentManualScaleAndAutoscaler validates §9 concurrency requirements:
// A user manually sets replica count at the exact same moment the autoscaler independently
// decides to scale -> confirm no lost update, deterministic behavior, and zero race conditions.
func TestAutoscaler_ConcurrentManualScaleAndAutoscaler(t *testing.T) {
	ctx := context.Background()
	scaler, metrics, svc, projectRepo, reg := setupAutoscalerTest(t)

	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-conc-1",
		Hostname:  "node-conc-1",
		Capacity:  50,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	projectID := "proj-concurrency-scaling"
	_ = projectRepo.Create(ctx, &projects.Project{
		ID:   projectID,
		Name: "Concurrent App",
	})

	dep, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "app:v1",
		InstanceCount: 2,
	})
	if err != nil {
		t.Fatalf("initial deploy: %v", err)
	}

	policy := &autoscaler.ScalingPolicy{
		ProjectID:         projectID,
		MetricType:        autoscaler.MetricCPU,
		TargetValue:       50.0,
		Tolerance:         0.05,
		MinReplicas:       1,
		MaxReplicas:       20,
		ScaleUpCooldown:   0, // Allow immediate evaluation in concurrency test
		ScaleDownCooldown: 0,
	}
	scaler.SetPolicy(policy)

	// Set high metric to trigger autoscaler scale-up to 4
	metrics.SetMetric(projectID, autoscaler.MetricCPU, 100.0)

	var wg sync.WaitGroup
	wg.Add(2)

	var errManual error
	var errAuto error

	startBarrier := make(chan struct{})

	// Goroutine 1: User manually sets replica count to 6
	go func() {
		defer wg.Done()
		<-startBarrier
		_, _, errManual = svc.ScaleDeployment(ctx, dep.ID, 6)
	}()

	// Goroutine 2: Autoscaler evaluates and scales concurrently
	go func() {
		defer wg.Done()
		<-startBarrier
		_, errAuto = scaler.EvaluateAndScale(ctx, projectID)
	}()

	// Release both goroutines simultaneously
	close(startBarrier)
	wg.Wait()

	if errManual != nil {
		t.Errorf("manual scale returned error: %v", errManual)
	}
	if errAuto != nil {
		t.Errorf("autoscaler returned error: %v", errAuto)
	}

	// Verify state consistency:
	// The deployment in DB must reflect one of the valid states (e.g. 4 or 6) without corruption
	finalDep, finalInstances, err := svc.GetDeployment(ctx, dep.ID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}

	var activeCount int
	for _, inst := range finalInstances {
		if inst.Status == "RUNNING" {
			activeCount++
		}
	}

	// No lost update: the stored InstanceCount MUST exactly match the number of running instances!
	if finalDep.InstanceCount != activeCount {
		t.Fatalf("lost update detected: deployment InstanceCount=%d does not match active running instances=%d",
			finalDep.InstanceCount, activeCount)
	}

	// Target count must be either 4 or 6 (the two concurrent target requests)
	if finalDep.InstanceCount != 4 && finalDep.InstanceCount != 6 {
		t.Errorf("unexpected final replica count: %d (expected 4 or 6)", finalDep.InstanceCount)
	}

	t.Logf("✅ Concurrency test passed: deterministic convergence to %d replicas with zero lost updates", finalDep.InstanceCount)
}
