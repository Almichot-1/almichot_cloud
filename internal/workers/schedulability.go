package workers

// EvaluationResult represents whether a worker can be scheduled.
type EvaluationResult struct {
	Allowed bool
	Reason  string
}

// EvaluateSchedulability checks if a worker is schedulable.
func (w *Worker) EvaluateSchedulability() EvaluationResult {
	if !w.Schedulable {
		return EvaluationResult{Allowed: false, Reason: "worker schedulable flag is false"}
	}
	if w.State == StateDraining {
		return EvaluationResult{Allowed: false, Reason: "worker is draining"}
	}
	if w.State != StateReady {
		return EvaluationResult{Allowed: false, Reason: "worker state is not ready"}
	}
	if w.Health == HealthUnhealthy || w.Health == HealthUnreachable || w.IsPartitioned {
		return EvaluationResult{Allowed: false, Reason: "worker is unhealthy or partitioned"}
	}
	if w.AvailableCapacity() <= 0 {
		return EvaluationResult{Allowed: false, Reason: "worker capacity exhausted"}
	}
	return EvaluationResult{Allowed: true, Reason: "worker is schedulable"}
}
