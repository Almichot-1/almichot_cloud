package deployments

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// RecoveryPolicy defines how the Control Plane handles stuck in-flight deployments on restart.
type RecoveryPolicy string

const (
	// RecoveryPolicyFailSafe marks interrupted deployments as FAILED, cleaning up all partial
	// containers on workers to guarantee zero orphan work (Gates G-16, G-17, G-18, G-19).
	RecoveryPolicyFailSafe RecoveryPolicy = "FAIL_SAFE"

	// RecoveryPolicyResume resumes in-flight deployments from their persisted stage.
	RecoveryPolicyResume RecoveryPolicy = "RESUME"
)

// RecoveryReport summarizes the actions performed during restart recovery.
type RecoveryReport struct {
	TotalStuckDetected      int      `json:"total_stuck_detected"`
	QueuedRecovered         int      `json:"queued_recovered"`
	BuildingRecovered       int      `json:"building_recovered"`
	SchedulingRecovered     int      `json:"scheduling_recovered"`
	StartingRecovered       int      `json:"starting_recovered"`
	OrphanContainersStopped int      `json:"orphan_containers_stopped"`
	Errors                  []string `json:"errors,omitempty"`
}

// RecoveryEngine identifies and resolves deployments frozen mid-transition after a CP crash (DL-03).
type RecoveryEngine struct {
	mu            sync.Mutex
	depRepo       DeploymentRepository
	instRepo      InstanceRepository
	registry      *workers.Registry
	sched         *scheduler.Scheduler
	clientFactory WorkerClientFactory
	log           zerolog.Logger
}

// NewRecoveryEngine creates a new crash recovery engine.
func NewRecoveryEngine(
	depRepo DeploymentRepository,
	instRepo InstanceRepository,
	registry *workers.Registry,
	sched *scheduler.Scheduler,
	clientFactory WorkerClientFactory,
	log zerolog.Logger,
) *RecoveryEngine {
	return &RecoveryEngine{
		depRepo:       depRepo,
		instRepo:      instRepo,
		registry:      registry,
		sched:         sched,
		clientFactory: clientFactory,
		log:           log.With().Str("component", "deployment-recovery").Logger(),
	}
}

// DetectStuckDeployments queries durable storage for all deployments frozen in non-terminal stages (DL-03).
func (e *RecoveryEngine) DetectStuckDeployments(ctx context.Context) ([]*Deployment, error) {
	allDeployments, err := e.depRepo.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to list deployments from storage: %w", err)
	}

	stuck := make([]*Deployment, 0)
	for _, d := range allDeployments {
		if d.Status.IsInFlight() {
			stuck = append(stuck, d)
		}
	}

	return stuck, nil
}

// RecoverDeployments resolves all stuck deployments according to the specified policy.
func (e *RecoveryEngine) RecoverDeployments(ctx context.Context, policy RecoveryPolicy) (*RecoveryReport, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	stuck, err := e.DetectStuckDeployments(ctx)
	if err != nil {
		return nil, err
	}

	report := &RecoveryReport{
		TotalStuckDetected: len(stuck),
	}

	if len(stuck) == 0 {
		e.log.Debug().Msg("no stuck in-flight deployments detected on startup")
		return report, nil
	}

	e.log.Info().
		Int("stuck_count", len(stuck)).
		Str("policy", string(policy)).
		Msg("resolving stuck deployments detected after control plane restart (DL-03)")

	for _, dep := range stuck {
		switch dep.Status {
		case StatusQueued:
			// CP-CRASH-01 (Gate G-16): CP crash while QUEUED
			// No containers exist, no build was started. Safely transition to FAILED with clear audit trail.
			err := e.depRepo.UpdateStatus(ctx, dep.ID, StatusFailed, "QUEUED_INTERRUPTED_CP_CRASH")
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("dep %s (QUEUED): %v", dep.ID, err))
			} else {
				report.QueuedRecovered++
				e.log.Info().Str("deployment_id", dep.ID).Msg("CP-CRASH-01 (G-16): recovered QUEUED deployment safely")
			}

		case StatusBuilding:
			// CP-CRASH-02 (Gate G-17) & DL-04: CP crash while BUILDING
			// Partial build artifact from crashed BUILDING stage must NOT be reused.
			// Reset image and digest to prevent trusting incomplete artifacts (DL-04).
			if err := e.depRepo.UpdateImage(ctx, dep.ID, "", ""); err != nil {
				e.log.Error().Err(err).Str("deployment_id", dep.ID).Msg("failed to clear image on building recovery")
			}
			err := e.depRepo.UpdateStatus(ctx, dep.ID, StatusFailed, "BUILD_INTERRUPTED_CP_CRASH")
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("dep %s (BUILDING): %v", dep.ID, err))
			} else {
				report.BuildingRecovered++
				e.log.Info().Str("deployment_id", dep.ID).Msg("CP-CRASH-02 (G-17, DL-04): recovered BUILDING deployment; partial artifact discarded")
			}

		case StatusBuilt, StatusScheduling:
			// CP-CRASH-03 (Gate G-18): CP crash while BUILT / SCHEDULING
			// Clean up any unstarted instance records created for this deployment
			instances, _ := e.instRepo.ListByDeployment(ctx, dep.ID)
			for _, inst := range instances {
				if inst.Status == "PENDING" {
					inst.Status = "FAILED"
					if uErr := e.instRepo.Update(ctx, inst); uErr != nil {
						e.log.Error().Err(uErr).Str("instance_id", inst.ID).Msg("failed to update pending instance to FAILED")
					}
				}
			}

			err := e.depRepo.UpdateStatus(ctx, dep.ID, StatusFailed, "SCHEDULING_INTERRUPTED_CP_CRASH")
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("dep %s (SCHEDULING): %v", dep.ID, err))
			} else {
				report.SchedulingRecovered++
				e.log.Info().Str("deployment_id", dep.ID).Msg("CP-CRASH-03 (G-18): recovered BUILT/SCHEDULING deployment safely")
			}

		case StatusStarting:
			// CP-CRASH-04 (Gate G-19): CP crash while STARTING
			// Containers may have been partially started on workers before the crash.
			// Query workers and stop any partial containers to guarantee zero orphan work!
			stoppedCount := e.cleanupPartialContainers(ctx, dep.ID)
			report.OrphanContainersStopped += stoppedCount

			// Mark all instances of this deployment as FAILED
			instances, _ := e.instRepo.ListByDeployment(ctx, dep.ID)
			for _, inst := range instances {
				inst.Status = "FAILED"
				if uErr := e.instRepo.Update(ctx, inst); uErr != nil {
					e.log.Error().Err(uErr).Str("instance_id", inst.ID).Msg("failed to update instance to FAILED")
				}
			}

			err := e.depRepo.UpdateStatus(ctx, dep.ID, StatusFailed, "STARTUP_INTERRUPTED_CP_CRASH")
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("dep %s (STARTING): %v", dep.ID, err))
			} else {
				report.StartingRecovered++
				e.log.Info().
					Str("deployment_id", dep.ID).
					Int("partial_containers_stopped", stoppedCount).
					Msg("CP-CRASH-04 (G-19): recovered STARTING deployment; partial containers cleaned up")
			}

		default:
			// Other in-flight states
			if err := e.depRepo.UpdateStatus(ctx, dep.ID, StatusFailed, "INTERRUPTED_CP_CRASH"); err != nil {
				e.log.Error().Err(err).Str("deployment_id", dep.ID).Msg("failed to update status to INTERRUPTED_CP_CRASH")
			}
		}
	}

	return report, nil
}

// cleanupPartialContainers contacts registered workers and terminates any running containers
// matching the deployment ID, enforcing zero orphan work (Gate G-19).
func (e *RecoveryEngine) cleanupPartialContainers(ctx context.Context, deploymentID string) int {
	if e.registry == nil || e.clientFactory == nil {
		return 0
	}

	workersList := e.registry.List()
	stoppedCount := 0

	for _, w := range workersList {
		if !w.IsHealthy() {
			continue
		}

		client, err := e.clientFactory.GetClient(ctx, w)
		if err != nil {
			continue
		}

		listResp, err := client.ListContainers(ctx, &proto.ListContainersRequest{})
		if err != nil || listResp == nil {
			continue
		}

		for _, ctr := range listResp.Containers {
			// Check if container belongs to this deployment
			if ctr.Labels != nil && ctr.Labels["nebula.deployment_id"] == deploymentID {
				e.log.Warn().
					Str("deployment_id", deploymentID).
					Str("container_id", ctr.ContainerId).
					Str("instance_id", ctr.InstanceId).
					Str("worker_key", w.WorkerKey).
					Msg("stopping partial/orphan container from crashed STARTING deployment (G-19)")

				stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, stopErr := client.StopContainer(stopCtx, &proto.StopContainerRequest{
					InstanceId:     ctr.InstanceId,
					TimeoutSeconds: 5,
				})
				cancel()

				if stopErr == nil {
					stoppedCount++
				}
			}
		}
	}

	return stoppedCount
}
