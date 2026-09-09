package workers

// IsDraining returns true if the worker is marked as DRAINING.
func (w *Worker) IsDraining() bool {
	return w.State == StateDraining
}

// CanAcceptWork returns true if the worker is eligible to accept new container workloads.
func (w *Worker) CanAcceptWork() bool {
	return w.Schedulable && w.State != StateDraining && w.Health != HealthUnhealthy && w.AvailableCapacity() > 0
}
