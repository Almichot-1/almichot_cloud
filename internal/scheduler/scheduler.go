package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

var (
	ErrNoWorkersRegistered = errors.New("no workers registered in cluster")
	ErrNoFeasibleWorkers   = errors.New("no feasible workers available for placement")
)

// InstanceCountProvider returns a map of worker ID/key to count of instances for a deployment.
type InstanceCountProvider func(ctx context.Context, deploymentID string) (map[string]int, error)

// Scheduler coordinates placement decisions according to explicit priority rules (G-06, G-07, G-08).
type Scheduler struct {
	registry       *workers.Registry
	filter         *FeasibilityFilter
	ranker         *Ranker
	countsProvider InstanceCountProvider
	log            zerolog.Logger
}

// NewScheduler constructs a new Scheduler.
func NewScheduler(registry *workers.Registry, countsProvider InstanceCountProvider, log zerolog.Logger) *Scheduler {
	return &Scheduler{
		registry:       registry,
		filter:         NewFeasibilityFilter(),
		ranker:         NewRanker(PolicySpreading),
		countsProvider: countsProvider,
		log:            log.With().Str("component", "scheduler").Logger(),
	}
}

// SelectWorker executes the scheduling pipeline enforcing strict priority order (G-06):
// Stage 1: Feasibility filter (healthy, capacity, not draining) [G-07]
// Stage 2: Spreading rank (anti-affinity per deployment) [G-08]
// Stage 3: Tie-break by lowest total active workloads, then deterministic key
func (s *Scheduler) SelectWorker(ctx context.Context, req WorkloadRequirement) (*workers.Worker, error) {
	// Step 1: Fetch candidate workers
	allWorkers := s.registry.List()
	if len(allWorkers) == 0 {
		return nil, ErrNoWorkersRegistered
	}

	// Step 2: Stage 1 - Feasibility Filter (G-07)
	feasible := s.filter.Filter(allWorkers, req)
	if len(feasible) == 0 {
		s.log.Warn().
			Int("total_workers", len(allWorkers)).
			Str("deployment_id", req.DeploymentID).
			Msg("placement failed: no feasible workers")
		return nil, fmt.Errorf("%w: %d candidates evaluated", ErrNoFeasibleWorkers, len(allWorkers))
	}

	// Step 3: Fetch deployment instance counts if provider is available
	var deploymentCounts map[string]int
	if s.countsProvider != nil && req.DeploymentID != "" {
		counts, err := s.countsProvider(ctx, req.DeploymentID)
		if err == nil {
			deploymentCounts = counts
		}
	}

	// Step 4: Stage 2 & 3 - Spreading Rank + Workload Tie-break (G-08)
	ranked := s.ranker.Rank(feasible, deploymentCounts)

	chosen := ranked[0]
	s.log.Info().
		Str("deployment_id", req.DeploymentID).
		Str("chosen_worker_key", chosen.WorkerKey).
		Str("chosen_worker_id", chosen.ID).
		Int("active_workloads", chosen.ActiveWorkloads).
		Int("capacity", chosen.Capacity).
		Msg("placement decision completed")

	return chosen, nil
}
