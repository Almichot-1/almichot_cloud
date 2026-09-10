package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// RepairResult represents the outcome of repairing a missing instance.
type RepairResult struct {
	InstanceKey string `json:"instance_key"`
	WorkerID    string `json:"worker_id"`
	ContainerID string `json:"container_id"`
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
}

// RepairService handles the self-healing recreation of missing instances (G-01, G-11, G-23).
type RepairService struct {
	registry      *workers.Registry
	depRepo       deployments.DeploymentRepository
	instRepo      deployments.InstanceRepository
	sched         *scheduler.Scheduler
	clientFactory deployments.WorkerClientFactory
	repairTimeout time.Duration
	log           zerolog.Logger
}

// NewRepairService creates a new RepairService.
func NewRepairService(
	registry *workers.Registry,
	depRepo deployments.DeploymentRepository,
	instRepo deployments.InstanceRepository,
	sched *scheduler.Scheduler,
	clientFactory deployments.WorkerClientFactory,
	repairTimeout time.Duration,
	log zerolog.Logger,
) *RepairService {
	if repairTimeout <= 0 {
		repairTimeout = 15 * time.Second
	}
	return &RepairService{
		registry:      registry,
		depRepo:       depRepo,
		instRepo:      instRepo,
		sched:         sched,
		clientFactory: clientFactory,
		repairTimeout: repairTimeout,
		log:           log.With().Str("component", "repair_service").Logger(),
	}
}

// RepairMissing recreates a missing instance in a controlled manner.
// Enforces:
// - G-01: External delete of managed container -> recreated within timeout.
// - G-11: Reconciliation never overshoots desired replica count.
// - G-23: docker rm -f on a managed container recreated on next cycle.
// - RCN-02: Configurable timeout enforced; non-converging repair is flagged with error.
func (s *RepairService) RepairMissing(
	ctx context.Context,
	missing DesiredInstance,
	currentlyRunningCount int,
	desiredCount int,
) (*RepairResult, error) {
	// G-11 invariant check: Never overshoot desired replica count
	if currentlyRunningCount >= desiredCount {
		s.log.Warn().
			Str("instance_key", missing.InstanceKey).
			Int("running", currentlyRunningCount).
			Int("desired", desiredCount).
			Msg("skipping recreation: running replica count already meets or exceeds desired count (G-11 invariant)")
		return &RepairResult{
			InstanceKey: missing.InstanceKey,
			Success:     false,
			Error:       "replica count invariant: running count >= desired count",
		}, nil
	}

	// Apply configurable timeout (RCN-02)
	repairCtx, cancel := context.WithTimeout(ctx, s.repairTimeout)
	defer cancel()

	// 1. Determine target worker:
	// If the originally assigned worker exists, is healthy, and schedulable, keep it on that worker.
	var targetWorker *workers.Worker
	if missing.WorkerID != "" {
		if w, ok := s.registry.Get(missing.WorkerID); ok && w.Health == workers.HealthHealthy && w.State != workers.StateDraining {
			targetWorker = w
		}
	}

	// If the assigned worker is invalid, offline, or draining, re-schedule via scheduler
	if targetWorker == nil {
		var err error
		targetWorker, err = s.sched.SelectWorker(repairCtx, scheduler.WorkloadRequirement{
			DeploymentID:     missing.DeploymentID,
			RequiredCapacity: 1,
			RequiredLabels:   missing.Labels,
		})
		if err != nil {
			s.log.Error().
				Err(err).
				Str("instance_key", missing.InstanceKey).
				Msg("failed to find feasible replacement worker during repair")
			return &RepairResult{
				InstanceKey: missing.InstanceKey,
				Success:     false,
				Error:       fmt.Sprintf("no feasible worker available: %v", err),
			}, err
		}
	}

	// 2. Obtain client connection to target worker
	client, err := s.clientFactory.GetClient(repairCtx, targetWorker)
	if err != nil {
		s.log.Error().
			Err(err).
			Str("worker_id", targetWorker.ID).
			Msg("failed to obtain worker client during repair")
		return &RepairResult{
			InstanceKey: missing.InstanceKey,
			WorkerID:    targetWorker.ID,
			Success:     false,
			Error:       fmt.Sprintf("dial worker failed: %v", err),
		}, err
	}

	// 3. Dispatch RunContainer to recreate the missing container
	labels := make(map[string]string)
	for k, v := range missing.Labels {
		labels[k] = v
	}
	labels["nebula.instance_id"] = missing.InstanceKey
	if missing.DeploymentID != "" {
		labels["nebula.deployment_id"] = missing.DeploymentID
	}

	runResp, err := client.RunContainer(repairCtx, &proto.RunContainerRequest{
		InstanceId:   missing.InstanceKey,
		DeploymentId: missing.DeploymentID,
		Image:        missing.Image,
		Env:          missing.Env,
		Labels:       labels,
	})
	if err != nil || (runResp != nil && runResp.Error != "") {
		errMsg := "failed to run container"
		if err != nil {
			errMsg = err.Error()
		} else if runResp != nil && runResp.Error != "" {
			errMsg = runResp.Error
		}

		s.log.Error().
			Str("instance_key", missing.InstanceKey).
			Str("err", errMsg).
			Msg("failed to recreate missing container on worker")

		// Mark instance status as REPAIR_FAILED in repository
		if inst, getErr := s.instRepo.GetByInstanceKey(repairCtx, missing.InstanceKey); getErr == nil {
			inst.Status = "REPAIR_FAILED"
			if updErr := s.instRepo.Update(repairCtx, inst); updErr != nil {
				s.log.Error().Err(updErr).Str("instance_key", missing.InstanceKey).Msg("failed to update instance status to REPAIR_FAILED")
			}
		} else {
			s.log.Error().Err(getErr).Str("instance_key", missing.InstanceKey).Msg("failed to get instance to set REPAIR_FAILED")
		}

		return &RepairResult{
			InstanceKey: missing.InstanceKey,
			WorkerID:    targetWorker.ID,
			Success:     false,
			Error:       errMsg,
		}, fmt.Errorf("recreate container failed: %s", errMsg)
	}

	// 4. Update instance record in repository
	if inst, err := s.instRepo.GetByInstanceKey(repairCtx, missing.InstanceKey); err == nil {
		inst.WorkerID = targetWorker.ID
		inst.Status = "RUNNING"
		if updErr := s.instRepo.Update(repairCtx, inst); updErr != nil {
			s.log.Error().Err(updErr).Str("instance_key", missing.InstanceKey).Msg("failed to update instance status to RUNNING")
		}
	} else {
		s.log.Error().Err(err).Str("instance_key", missing.InstanceKey).Msg("failed to get instance to set RUNNING")
	}

	s.log.Info().
		Str("instance_key", missing.InstanceKey).
		Str("worker_id", targetWorker.ID).
		Str("container_id", runResp.ContainerId).
		Msg("successfully repaired missing instance (G-01, G-23)")

	return &RepairResult{
		InstanceKey: missing.InstanceKey,
		WorkerID:    targetWorker.ID,
		ContainerID: runResp.ContainerId,
		Success:     true,
	}, nil
}
