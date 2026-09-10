package scheduler

import (
	"github.com/nebula/nebula/internal/workers"
)

// FeasibilityFilter evaluates candidate workers against hard placement constraints.
// A worker must pass all feasibility checks to be considered for ranking.
type FeasibilityFilter struct{}

// NewFeasibilityFilter creates a new FeasibilityFilter.
func NewFeasibilityFilter() *FeasibilityFilter {
	return &FeasibilityFilter{}
}

// Filter eliminates ineligible workers according to explicit feasibility rules (G-07):
// 1. Worker must be schedulable (schedulable flag must be true).
// 2. Worker must NOT be DRAINING (G-07).
// 3. Worker must be in READY state.
// 4. Worker must NOT be UNHEALTHY.
// 5. Worker must have available capacity (Capacity - ActiveWorkloads >= required).
// 6. Worker must satisfy required labels (if specified).
func (f *FeasibilityFilter) Filter(candidates []*workers.Worker, req WorkloadRequirement) []*workers.Worker {
	var feasible []*workers.Worker

	requiredCap := req.RequiredCapacity
	if requiredCap <= 0 {
		requiredCap = 1
	}

	for _, w := range candidates {
		// Rule 1 & 2: Draining and schedulability exclusion (G-07)
		if !w.Schedulable || w.State == workers.StateDraining {
			continue
		}

		// Rule 3: Must be in READY state
		if w.State != workers.StateReady {
			continue
		}

		// Rule 4: Must not be unhealthy
		if w.Health == workers.HealthUnhealthy {
			continue
		}

		// Rule 5: Capacity check
		if w.Capacity-w.ActiveWorkloads < requiredCap {
			continue
		}

		// Rule 6: Required labels check
		if len(req.RequiredLabels) > 0 {
			matches := true
			for k, v := range req.RequiredLabels {
				if w.Labels[k] != v {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}
		}

		// Rule 7: Build vs Runtime fleet separation (§9.3, §14, Phase 12)
		// Build workers (capability=build) only accept build workloads;
		// Runtime workers only accept runtime workloads.
		isBuildReq := req.RequiredLabels != nil && req.RequiredLabels["capability"] == workers.CapabilityBuild
		isBuildWorker := w.IsBuildWorker()
		if isBuildReq && !isBuildWorker {
			continue
		}
		if !isBuildReq && isBuildWorker {
			continue
		}

		feasible = append(feasible, w)
	}

	return feasible
}
