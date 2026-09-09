package workers

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// WA-09: Health state machine transitions: HEALTHY → SUSPECTED → UNHEALTHY.
// Transitions only occur after the correct missed-heartbeat thresholds.
func TestWA09_HealthStateMachine_Transitions(t *testing.T) {
	cfg := HealthStateMachineConfig{
		HeartbeatInterval:      1 * time.Second,
		SuspectedThreshold:     2,
		UnhealthyThreshold:     3,
		ConsecutiveBeatsToHeal: 2,
	}

	cases := []struct {
		name           string
		elapsed        time.Duration
		expectedHealth WorkerHealth
		expectedMissed int
	}{
		{
			name:           "0 missed beats -> HEALTHY",
			elapsed:        500 * time.Millisecond,
			expectedHealth: HealthHealthy,
			expectedMissed: 0,
		},
		{
			name:           "1 missed beat -> still HEALTHY",
			elapsed:        1500 * time.Millisecond,
			expectedHealth: HealthHealthy,
			expectedMissed: 1,
		},
		{
			name:           "2 missed beats -> SUSPECTED",
			elapsed:        2100 * time.Millisecond,
			expectedHealth: HealthSuspected,
			expectedMissed: 2,
		},
		{
			name:           "3 missed beats -> UNHEALTHY",
			elapsed:        3100 * time.Millisecond,
			expectedHealth: HealthUnhealthy,
			expectedMissed: 3,
		},
		{
			name:           "5 missed beats -> UNHEALTHY",
			elapsed:        5000 * time.Millisecond,
			expectedHealth: HealthUnhealthy,
			expectedMissed: 5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			newHealth, missed := EvaluateHealthTransition(HealthHealthy, tc.elapsed, cfg)
			if newHealth != tc.expectedHealth {
				t.Fatalf("WA-09 failure: for elapsed %v, expected health %s, got %s",
					tc.elapsed, tc.expectedHealth, newHealth)
			}
			if missed != tc.expectedMissed {
				t.Fatalf("WA-09 failure: expected missed beats %d, got %d", tc.expectedMissed, missed)
			}
		})
	}

	t.Logf("WA-09 Passed: Health transitions (HEALTHY -> SUSPECTED -> UNHEALTHY) strictly adhere to missed heartbeat thresholds!")
}

// FS-07: Flapping worker (heartbeat intermittently missed) doesn't cause thrash (hysteresis enforced).
func TestFS07_FlappingWorker_HysteresisPreventsThrash(t *testing.T) {
	cfg := HealthStateMachineConfig{
		HeartbeatInterval:      1 * time.Second,
		SuspectedThreshold:     2,
		UnhealthyThreshold:     3,
		ConsecutiveBeatsToHeal: 2, // Needs 2 consecutive beats to heal!
	}

	w := &Worker{
		WorkerKey: "flapping-worker",
		Health:    HealthUnhealthy,
	}

	// Beat 1 arrives: ConsecutiveBeats becomes 1, which is < 2.
	// Worker must NOT prematurely jump back to HEALTHY!
	h1 := ApplyHeartbeatSuccess(w, cfg)
	if h1 == HealthHealthy {
		t.Fatalf("FS-07 failure: single heartbeat prematurely restored HEALTHY on flapping worker!")
	}
	if w.ConsecutiveBeats != 1 {
		t.Fatalf("expected 1 consecutive beat, got %d", w.ConsecutiveBeats)
	}

	// Beat 2 arrives consecutively: ConsecutiveBeats becomes 2 >= 2.
	// Now hysteresis threshold is satisfied; worker safely transitions to HEALTHY.
	h2 := ApplyHeartbeatSuccess(w, cfg)
	if h2 != HealthHealthy {
		t.Fatalf("FS-07 failure: expected 2 consecutive heartbeats to restore HEALTHY, got %s", h2)
	}

	t.Logf("FS-07 Passed: Hysteresis prevented premature health recovery on flapping worker!")
}

// Verification: Health and schedulability are tracked as separate axes.
// (A DRAINING worker can still be HEALTHY; an UNHEALTHY worker is excluded from placement).
func TestSeparateAxes_HealthAndSchedulability(t *testing.T) {
	reg := NewRegistry(NewMemoryWorkerRepository(), zerolog.Nop())
	ctx := context.Background()

	w, err := reg.Register(ctx, RegisterParams{
		WorkerKey: "separate-axes-worker",
		Capacity:  5,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// 1. Initially READY and HEALTHY
	eval := w.EvaluateSchedulability()
	if !eval.Allowed {
		t.Fatalf("expected allowed, got false: %s", eval.Reason)
	}
	if !w.IsHealthy() {
		t.Fatalf("expected worker to be healthy")
	}

	// 2. Drain worker: state becomes DRAINING, but health remains HEALTHY!
	drainedW, err := reg.Drain(ctx, w.WorkerKey)
	if err != nil {
		t.Fatalf("failed to drain worker: %v", err)
	}
	if drainedW.State != StateDraining {
		t.Fatalf("expected state DRAINING, got %s", drainedW.State)
	}
	if drainedW.Health != HealthHealthy {
		t.Fatalf("separate axes failure: DRAINING worker health must remain HEALTHY, got %s", drainedW.Health)
	}
	evalDrained := drainedW.EvaluateSchedulability()
	if evalDrained.Allowed {
		t.Fatalf("separate axes failure: DRAINING worker must be excluded from scheduling")
	}

	// 3. Worker marked UNHEALTHY
	unhealthyW, err := reg.SetHealthWithReason(ctx, w.WorkerKey, HealthUnhealthy, "heartbeat lost")
	if err != nil {
		t.Fatalf("failed to set health: %v", err)
	}
	if unhealthyW.IsHealthy() {
		t.Fatalf("expected worker to not be healthy")
	}
	evalUnhealthy := unhealthyW.EvaluateSchedulability()
	if evalUnhealthy.Allowed {
		t.Fatalf("separate axes failure: UNHEALTHY worker must be excluded from scheduling")
	}

	t.Logf("Passed: Health and schedulability confirmed as separate orthogonal axes!")
}
