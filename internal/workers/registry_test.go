package workers

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestWorkerRegistry_Lifecycle tests general registration and workload accounting.
func TestWorkerRegistry_Lifecycle(t *testing.T) {
	log := zerolog.Nop()
	repo := NewMemoryWorkerRepository()
	reg := NewRegistry(repo, log)
	ctx := context.Background()

	w, err := reg.Register(ctx, RegisterParams{
		WorkerKey: "worker-01",
		Hostname:  "host-01",
		IPAddress: "192.168.1.50",
		GRPCPort:  9091,
		Capacity:  4,
		Labels:    map[string]string{"env": "production"},
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if w.State != StateReady || !w.Schedulable {
		t.Fatalf("expected state READY and Schedulable=true")
	}

	reg.UpdateWorkload("worker-01", 2)
	wWorkload, _ := reg.Get("worker-01")
	if wWorkload.ActiveWorkloads != 2 || wWorkload.AvailableCapacity() != 2 {
		t.Fatalf("unexpected workload state: active=%d, avail=%d",
			wWorkload.ActiveWorkloads, wWorkload.AvailableCapacity())
	}
}

// CP-02: Heartbeat recorded, updates last_heartbeat_at strictly advancing on successive calls.
func TestCP02_HeartbeatAdvancesTimestamp(t *testing.T) {
	log := zerolog.Nop()
	repo := NewMemoryWorkerRepository()
	reg := NewRegistry(repo, log)
	ctx := context.Background()

	w, err := reg.Register(ctx, RegisterParams{
		WorkerKey: "worker-hb",
		Capacity:  5,
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// First heartbeat
	if err := reg.Heartbeat(ctx, w.WorkerKey); err != nil {
		t.Fatalf("first Heartbeat failed: %v", err)
	}
	w1, _ := reg.Get(w.WorkerKey)
	if w1.LastBeatAt == nil {
		t.Fatalf("expected LastBeatAt to be non-nil after first heartbeat")
	}
	t1 := *w1.LastBeatAt

	// Small delay to ensure timestamp difference
	time.Sleep(15 * time.Millisecond)

	// Second heartbeat
	if err := reg.Heartbeat(ctx, w.WorkerKey); err != nil {
		t.Fatalf("second Heartbeat failed: %v", err)
	}
	w2, _ := reg.Get(w.WorkerKey)
	if w2.LastBeatAt == nil {
		t.Fatalf("expected LastBeatAt to be non-nil after second heartbeat")
	}
	t2 := *w2.LastBeatAt

	if !t2.After(t1) {
		t.Fatalf("CP-02 failure: second heartbeat timestamp (%v) is not strictly after first timestamp (%v)", t2, t1)
	}
	t.Logf("CP-02 Passed: Heartbeat timestamp strictly advanced from %v to %v (delta: %v)", t1, t2, t2.Sub(t1))
}

// DRN-01: DrainWorker sets schedulability without touching health.
func TestDRN01_DrainWorker_PreservesHealth(t *testing.T) {
	log := zerolog.Nop()
	repo := NewMemoryWorkerRepository()
	reg := NewRegistry(repo, log)
	ctx := context.Background()

	w, err := reg.Register(ctx, RegisterParams{
		WorkerKey: "worker-drain-test",
		Capacity:  5,
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if w.Health != HealthHealthy {
		t.Fatalf("expected initial health HEALTHY, got %s", w.Health)
	}

	// Drain worker
	drained, err := reg.Drain(ctx, w.WorkerKey)
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	// Schedulability and state flip
	if drained.State != StateDraining {
		t.Fatalf("expected State DRAINING, got %s", drained.State)
	}
	if drained.Schedulable {
		t.Fatalf("expected Schedulable=false")
	}

	// DRN-01 assertion: Health must remain untouched!
	if drained.Health != HealthHealthy {
		t.Fatalf("DRN-01 failure: worker health was modified during drain! Got %s, expected HEALTHY", drained.Health)
	}

	// Verify in registry lookup as well
	fromReg, _ := reg.Get(w.WorkerKey)
	if fromReg.Health != HealthHealthy {
		t.Fatalf("DRN-01 failure in registry: expected HEALTHY, got %s", fromReg.Health)
	}

	t.Log("DRN-01 Passed: DrainWorker flipped schedulability to DRAINING while preserving Health == HEALTHY untouched!")
}
