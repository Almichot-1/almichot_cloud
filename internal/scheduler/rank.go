package scheduler

import (
	"math"
	"sort"

	"github.com/nebula/nebula/internal/workers"
)

// Ranker scores and orders feasible workers according to scheduling policies.
type Ranker struct {
	policy SchedulingPolicy
}

// NewRanker creates a new Ranker with the given policy.
func NewRanker(policy SchedulingPolicy) *Ranker {
	if policy == "" {
		policy = PolicySpreading
	}
	return &Ranker{policy: policy}
}

// Rank sorts feasible workers in-place or returns a ranked slice.
// Priority order for spreading default (G-06, G-08, SC-04):
// 1. Spreading: Fewer instances of this deployment on the worker (anti-affinity).
// 2. Utilization tie-break: Lowest utilization ratio (ActiveWorkloads / Capacity).
// 3. Absolute workload count: Lowest total ActiveWorkloads.
// 4. Deterministic tie-break: WorkerKey lexicographically.
func (r *Ranker) Rank(feasible []*workers.Worker, deploymentInstanceCounts map[string]int) []*workers.Worker {
	if len(feasible) <= 1 {
		return feasible
	}

	ranked := make([]*workers.Worker, len(feasible))
	copy(ranked, feasible)

	sort.SliceStable(ranked, func(i, j int) bool {
		w1, w2 := ranked[i], ranked[j]

		// Priority 1 (Spreading): Deployment-level instance count
		count1 := 0
		count2 := 0
		if deploymentInstanceCounts != nil {
			count1 = deploymentInstanceCounts[w1.ID]
			if count1 == 0 {
				count1 = deploymentInstanceCounts[w1.WorkerKey]
			}
			count2 = deploymentInstanceCounts[w2.ID]
			if count2 == 0 {
				count2 = deploymentInstanceCounts[w2.WorkerKey]
			}
		}

		if count1 != count2 {
			return count1 < count2 // Fewer instances of this deployment is preferred
		}

		// Priority 2 (Utilization tie-break - SC-04): Lowest utilization ratio wins
		u1 := w1.Utilization()
		u2 := w2.Utilization()
		const epsilon = 1e-9
		if math.Abs(u1-u2) > epsilon {
			return u1 < u2 // Lower utilization wins
		}

		// Priority 3 (Workload count): Total active workloads
		if w1.ActiveWorkloads != w2.ActiveWorkloads {
			return w1.ActiveWorkloads < w2.ActiveWorkloads
		}

		// Priority 4 (Deterministic tie-breaker): WorkerKey
		return w1.WorkerKey < w2.WorkerKey
	})

	return ranked
}
