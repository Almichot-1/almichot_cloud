package scheduler

import (
	"context"
	"testing"

	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// SC-01: Feasibility filter excludes unhealthy workers.
func TestSC01_FeasibilityFilter_ExcludesUnhealthy(t *testing.T) {
	filter := NewFeasibilityFilter()
	candidates := []*workers.Worker{
		{
			WorkerKey:   "worker-unhealthy",
			State:       workers.StateReady,
			Health:      workers.HealthUnhealthy,
			Capacity:    10,
			Schedulable: true,
		},
		{
			WorkerKey:   "worker-healthy",
			State:       workers.StateReady,
			Health:      workers.HealthHealthy,
			Capacity:    10,
			Schedulable: true,
		},
	}

	feasible := filter.Filter(candidates, WorkloadRequirement{RequiredCapacity: 1})
	if len(feasible) != 1 {
		t.Fatalf("expected 1 feasible worker, got %d", len(feasible))
	}
	if feasible[0].WorkerKey != "worker-healthy" {
		t.Fatalf("expected worker-healthy, got %s", feasible[0].WorkerKey)
	}
}

// SC-02: Feasibility filter excludes over-capacity workers directly at filter level.
func TestSC02_FeasibilityFilter_ExcludesOverCapacity(t *testing.T) {
	filter := NewFeasibilityFilter()
	candidates := []*workers.Worker{
		{
			WorkerKey:       "worker-full",
			State:           workers.StateReady,
			Health:          workers.HealthHealthy,
			Capacity:        2,
			ActiveWorkloads: 2, // 2 - 2 = 0 available
			Schedulable:     true,
		},
		{
			WorkerKey:       "worker-has-room",
			State:           workers.StateReady,
			Health:          workers.HealthHealthy,
			Capacity:        5,
			ActiveWorkloads: 4, // 5 - 4 = 1 available
			Schedulable:     true,
		},
	}

	feasible := filter.Filter(candidates, WorkloadRequirement{RequiredCapacity: 1})
	if len(feasible) != 1 {
		t.Fatalf("expected 1 feasible worker, got %d", len(feasible))
	}
	if feasible[0].WorkerKey != "worker-has-room" {
		t.Fatalf("expected worker-has-room, got %s", feasible[0].WorkerKey)
	}
}

// SC-03: Spreading default with equal-capacity workers (G-08).
func TestSC03_SpreadingDefault(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	ctx := context.Background()

	w1, _ := reg.Register(ctx, workers.RegisterParams{WorkerKey: "worker-1", Capacity: 5})
	reg.UpdateWorkload(w1.WorkerKey, 1)

	w2, _ := reg.Register(ctx, workers.RegisterParams{WorkerKey: "worker-2", Capacity: 5})
	reg.UpdateWorkload(w2.WorkerKey, 0) // lowest workload

	sched := NewScheduler(reg, nil, log)
	chosen, err := sched.SelectWorker(ctx, WorkloadRequirement{RequiredCapacity: 1})
	if err != nil {
		t.Fatalf("scheduler failed: %v", err)
	}
	if chosen.WorkerKey != "worker-2" {
		t.Fatalf("expected lowest workload count worker-2, got %s", chosen.WorkerKey)
	}
}

// SC-04: Utilization tie-break: when workload counts are equal, lowest utilization ratio wins.
func TestSC04_UtilizationTieBreak(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	ctx := context.Background()

	// Worker A has 2 workloads on Capacity 10 -> Utilization = 20% (0.20)
	wA, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-A-big",
		Capacity:  10,
	})
	reg.UpdateWorkload(wA.WorkerKey, 2)

	// Worker B has 2 workloads on Capacity 4 -> Utilization = 50% (0.50)
	wB, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-B-small",
		Capacity:  4,
	})
	reg.UpdateWorkload(wB.WorkerKey, 2)

	// Workload counts are exactly equal (2 vs 2).
	sched := NewScheduler(reg, nil, log)
	chosen, err := sched.SelectWorker(ctx, WorkloadRequirement{RequiredCapacity: 1})
	if err != nil {
		t.Fatalf("scheduler failed: %v", err)
	}

	// Worker A must win because 20% < 50% utilization!
	if chosen.WorkerKey != "worker-A-big" {
		t.Fatalf("SC-04 failure: expected worker-A-big with lowest utilization (0.20), got %s (utilization %f)",
			chosen.WorkerKey, chosen.Utilization())
	}
	wAFresh, _ := reg.Get(wA.WorkerKey)
	wBFresh, _ := reg.Get(wB.WorkerKey)
	t.Logf("SC-04 Passed: worker-A-big (util: %.2f) beat worker-B-small (util: %.2f)",
		wAFresh.Utilization(), wBFresh.Utilization())
}

// SC-05: Placement constraints respected: workload with constraints only lands on matching workers.
func TestSC05_PlacementConstraintsRespected(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	ctx := context.Background()

	// Worker 1: zone=us-east
	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-east",
		Capacity:  10,
		Labels:    map[string]string{"zone": "us-east"},
	})

	// Worker 2: zone=us-west, gpu=true
	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-west-gpu",
		Capacity:  10,
		Labels:    map[string]string{"zone": "us-west", "gpu": "true"},
	})

	sched := NewScheduler(reg, nil, log)

	// Require zone=us-west and gpu=true
	chosen, err := sched.SelectWorker(ctx, WorkloadRequirement{
		RequiredCapacity: 1,
		RequiredLabels: map[string]string{
			"zone": "us-west",
			"gpu":  "true",
		},
	})
	if err != nil {
		t.Fatalf("scheduler failed: %v", err)
	}
	if chosen.WorkerKey != "worker-west-gpu" {
		t.Fatalf("SC-05 failure: expected worker-west-gpu matching constraints, got %s", chosen.WorkerKey)
	}
}

// SC-06..09: Full priority order enforced across a single mixed scenario (G-06):
// (1) healthy → (2) capacity → (3) constraints → (4) spreading → (5) utilization
func TestSC06_09_FullPriorityOrderEnforced(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	ctx := context.Background()

	const targetDep = "dep-mixed-priority"

	// Candidate 1: Unhealthy (Capacity 10, Util 0%) -> Filtered out by (1) Healthy
	w1, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "c1-unhealthy",
		Capacity:  10,
		Labels:    map[string]string{"tier": "gold"},
	})
	reg.SetHealth(w1.WorkerKey, workers.HealthUnhealthy)

	// Candidate 2: Over-capacity (Capacity 2, Workloads 2, Healthy, matches constraints) -> Filtered out by (2) Capacity
	w2, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "c2-overcapacity",
		Capacity:  2,
		Labels:    map[string]string{"tier": "gold"},
	})
	reg.UpdateWorkload(w2.WorkerKey, 2)

	// Candidate 3: Fails constraints (Capacity 10, Healthy, Workload 0, but tier=silver) -> Filtered out by (3) Constraints
	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "c3-wrong-tier",
		Capacity:  10,
		Labels:    map[string]string{"tier": "silver"},
	})

	// Candidate 4: Feasible, but has 1 instance of targetDep (Capacity 10, Workload 1 -> util 10%) -> Filtered out by (4) Spreading
	w4, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "c4-has-instance",
		Capacity:  10,
		Labels:    map[string]string{"tier": "gold"},
	})
	reg.UpdateWorkload(w4.WorkerKey, 1)

	// Candidate 5: Feasible, has 0 instances of targetDep, Capacity 4, Workload 1 -> Util 25% (0.25) -> Loses to Candidate 6 on (5) Utilization
	w5, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "c5-higher-util",
		Capacity:  4,
		Labels:    map[string]string{"tier": "gold"},
	})
	reg.UpdateWorkload(w5.WorkerKey, 1)

	// Candidate 6: Feasible, has 0 instances of targetDep, Capacity 10, Workload 1 -> Util 10% (0.10) -> WINS on (5) Utilization!
	w6, _ := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "c6-winner-lowest-util",
		Capacity:  10,
		Labels:    map[string]string{"tier": "gold"},
	})
	reg.UpdateWorkload(w6.WorkerKey, 1)

	// Instance count provider for targetDep:
	// c4 has 1 instance of targetDep; c5 and c6 have 0 instances of targetDep.
	countsProvider := func(ctx context.Context, deploymentID string) (map[string]int, error) {
		return map[string]int{
			"c4-has-instance":       1,
			"c5-higher-util":        0,
			"c6-winner-lowest-util": 0,
		}, nil
	}

	sched := NewScheduler(reg, countsProvider, log)

	chosen, err := sched.SelectWorker(ctx, WorkloadRequirement{
		DeploymentID:     targetDep,
		RequiredCapacity: 1,
		RequiredLabels:   map[string]string{"tier": "gold"},
	})
	if err != nil {
		t.Fatalf("scheduler failed: %v", err)
	}

	if chosen.WorkerKey != "c6-winner-lowest-util" {
		t.Fatalf("SC-06..09 failure: expected c6-winner-lowest-util to win, got %s", chosen.WorkerKey)
	}

	t.Log("SC-06..09 Passed: Full 5-stage priority order strictly enforced in mixed scenario!")
}

// SC-10: DRAINING worker excluded even if healthy (G-07).
func TestSC10_DrainingWorkerExcludedEvenIfHealthy(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	ctx := context.Background()

	w, _ := reg.Register(ctx, workers.RegisterParams{WorkerKey: "w-draining", Capacity: 10})
	_, _ = reg.Drain(ctx, w.WorkerKey)

	sched := NewScheduler(reg, nil, log)
	_, err := sched.SelectWorker(ctx, WorkloadRequirement{RequiredCapacity: 1})
	if err == nil {
		t.Fatalf("expected error when only worker is draining, got nil")
	}
}
