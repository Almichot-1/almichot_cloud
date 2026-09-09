package workers

import "time"

// HealthStateMachineConfig configures the thresholds and hysteresis for worker health.
type HealthStateMachineConfig struct {
	HeartbeatInterval      time.Duration
	SuspectedThreshold     int // Missed beats to transition to SUSPECTED (e.g. 2)
	UnhealthyThreshold     int // Missed beats to transition to UNHEALTHY (e.g. 3)
	ConsecutiveBeatsToHeal int // Consecutive successful heartbeats required to recover from SUSPECTED/UNHEALTHY to HEALTHY (e.g. 2)
}

// DefaultHealthConfig provides standard threshold defaults.
func DefaultHealthConfig() HealthStateMachineConfig {
	return HealthStateMachineConfig{
		HeartbeatInterval:      1 * time.Second,
		SuspectedThreshold:     2,
		UnhealthyThreshold:     3,
		ConsecutiveBeatsToHeal: 2,
	}
}

// EvaluateHealthTransition computes the new health state for a worker based on missed heartbeats (WA-09).
func EvaluateHealthTransition(currentHealth WorkerHealth, elapsedSinceLastBeat time.Duration, cfg HealthStateMachineConfig) (WorkerHealth, int) {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 1 * time.Second
	}

	missedBeats := int(elapsedSinceLastBeat / cfg.HeartbeatInterval)

	if missedBeats >= cfg.UnhealthyThreshold {
		return HealthUnhealthy, missedBeats
	}
	if missedBeats >= cfg.SuspectedThreshold {
		return HealthSuspected, missedBeats
	}

	return HealthHealthy, missedBeats
}

// ApplyHeartbeatSuccess updates hysteresis counters when a valid heartbeat is received (FS-07).
// Returns the resulting health state after considering hysteresis.
func ApplyHeartbeatSuccess(w *Worker, cfg HealthStateMachineConfig) WorkerHealth {
	w.ConsecutiveBeats++
	w.ConsecutiveMisses = 0

	// If currently UNHEALTHY or SUSPECTED, require ConsecutiveBeatsToHeal to prevent thrash/flapping
	if w.Health == HealthUnhealthy || w.Health == HealthSuspected || w.Health == HealthUnreachable {
		if w.ConsecutiveBeats >= cfg.ConsecutiveBeatsToHeal {
			w.Health = HealthHealthy
			w.IsPartitioned = false
		}
	} else {
		w.Health = HealthHealthy
	}

	return w.Health
}

// IsHealthy returns true if the worker is in healthy state.
func (w *Worker) IsHealthy() bool {
	return w.Health == HealthHealthy
}

// SecondsSinceLastHeartbeat returns elapsed seconds since the last recorded heartbeat.
func (w *Worker) SecondsSinceLastHeartbeat(now time.Time) float64 {
	if w.LastBeatAt == nil {
		return -1
	}
	return now.Sub(*w.LastBeatAt).Seconds()
}
