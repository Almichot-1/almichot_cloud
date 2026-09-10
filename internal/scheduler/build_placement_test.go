package scheduler

import (
	"context"
	"testing"

	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// TestBuildPlacement_MutualExclusion verifies that:
//  1. Build jobs route ONLY to build-tagged workers (capability=build) and never to runtime workers.
//  2. Runtime workloads route ONLY to runtime workers and never to build-tagged workers,
//     even when build-tagged workers have excess available capacity.
func TestBuildPlacement_MutualExclusion(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	sched := NewScheduler(reg, nil, log)

	// Register 2 Runtime Workers
	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "runtime-node-1",
		Hostname:  "run-1",
		IPAddress: "10.0.1.1",
		Capacity:  5,
		Labels:    map[string]string{"capability": "runtime"},
	})
	if err != nil {
		t.Fatalf("register runtime 1: %v", err)
	}

	_, err = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "runtime-node-2",
		Hostname:  "run-2",
		IPAddress: "10.0.1.2",
		Capacity:  5,
		Labels:    nil, // Default capability is runtime
	})
	if err != nil {
		t.Fatalf("register runtime 2: %v", err)
	}

	// Register 2 Build Workers
	_, err = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "build-node-1",
		Hostname:  "build-1",
		IPAddress: "10.0.2.1",
		Capacity:  10,
		Labels:    map[string]string{"capability": "build"},
	})
	if err != nil {
		t.Fatalf("register build 1: %v", err)
	}

	_, err = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "build-node-2",
		Hostname:  "build-2",
		IPAddress: "10.0.2.2",
		Capacity:  10,
		Labels:    map[string]string{"capability": "build"},
	})
	if err != nil {
		t.Fatalf("register build 2: %v", err)
	}

	// 1. Schedule runtime workload: must NEVER select build-node-1 or build-node-2
	for i := 0; i < 10; i++ {
		target, err := sched.SelectWorker(ctx, WorkloadRequirement{
			DeploymentID:     "dep-runtime",
			RequiredCapacity: 1,
		})
		if err != nil {
			t.Fatalf("select runtime worker: %v", err)
		}
		if target.IsBuildWorker() {
			t.Fatalf("VIOLATION: runtime workload scheduled on build worker %s!", target.WorkerKey)
		}
		if target.WorkerKey != "runtime-node-1" && target.WorkerKey != "runtime-node-2" {
			t.Fatalf("unexpected worker selected for runtime: %s", target.WorkerKey)
		}
	}

	// 2. Schedule build job: must ONLY select build-node-1 or build-node-2
	for i := 0; i < 10; i++ {
		target, err := sched.SelectWorker(ctx, WorkloadRequirement{
			RequiredCapacity: 1,
			RequiredLabels:   map[string]string{"capability": "build"},
		})
		if err != nil {
			t.Fatalf("select build worker: %v", err)
		}
		if !target.IsBuildWorker() {
			t.Fatalf("VIOLATION: build job scheduled on runtime worker %s!", target.WorkerKey)
		}
		if target.WorkerKey != "build-node-1" && target.WorkerKey != "build-node-2" {
			t.Fatalf("unexpected worker selected for build: %s", target.WorkerKey)
		}
	}

	// 3. Exhaust runtime workers: ensure error returned rather than falling back to build workers
	reg.UpdateWorkload("runtime-node-1", 5) // Full
	reg.UpdateWorkload("runtime-node-2", 5) // Full

	_, err = sched.SelectWorker(ctx, WorkloadRequirement{
		DeploymentID:     "dep-runtime-overflow",
		RequiredCapacity: 1,
	})
	if err == nil {
		t.Fatalf("expected ErrNoFeasibleWorkers when runtime fleet is full, got nil!")
	}

	// 4. Build workers can still accept build workloads
	buildTarget, err := sched.SelectWorker(ctx, WorkloadRequirement{
		RequiredCapacity: 1,
		RequiredLabels:   map[string]string{"capability": "build"},
	})
	if err != nil {
		t.Fatalf("expected build worker available, got err: %v", err)
	}
	if !buildTarget.IsBuildWorker() {
		t.Fatalf("expected build worker, got %s", buildTarget.WorkerKey)
	}
}
