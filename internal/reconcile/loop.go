package reconcile

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/discovery"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// ReconcileActions summarizes all state convergence actions performed in a reconciliation pass.
type ReconcileActions struct {
	RecreatedCount int      `json:"recreated_count"`
	StoppedCount   int      `json:"stopped_count"`
	FlaggedCount   int      `json:"flagged_count"`
	MigratedCount  int      `json:"migrated_count"`
	Errors         []string `json:"errors,omitempty"`
}

// IsZero returns true if no mutating or flagging actions were performed (G-02).
func (a *ReconcileActions) IsZero() bool {
	return a.RecreatedCount == 0 && a.StoppedCount == 0 && a.FlaggedCount == 0 && a.MigratedCount == 0
}

// TotalActions returns total mutating repair and cleanup actions performed.
func (a *ReconcileActions) TotalActions() int {
	return a.RecreatedCount + a.StoppedCount + a.MigratedCount
}

// Reconciler is the core state convergence engine.
// Periodically reconciles desired state vs. actual state across all registered workers:
// - Orphan container detection and cleanup (G-03, CP-04, CP-06).
// - Crash and missing container recreation (G-01, G-11, G-23, DA-03).
// - CP restart state alignment (G-04, G-24).
// - Strict replica count invariant (G-11).
// - Periodic convergence interval loop (RCN-01).
// - Configurable repair timeout (RCN-02).
// - Workload migration from DRAINING workers (DRN-02).
type Reconciler struct {
	registry        *workers.Registry
	depRepo         deployments.DeploymentRepository
	instRepo        deployments.InstanceRepository
	sched           *scheduler.Scheduler
	clientFactory   deployments.WorkerClientFactory
	repairService   *RepairService
	serviceRegistry *discovery.ServiceRegistry
	router          *loadbalancer.Router
	interval        time.Duration
	timeout         time.Duration
	log             zerolog.Logger

	mu     sync.Mutex
	stopCh chan struct{}
}

// NewReconciler creates a new Reconciler.
func NewReconciler(
	registry *workers.Registry,
	depRepo deployments.DeploymentRepository,
	instRepo deployments.InstanceRepository,
	sched *scheduler.Scheduler,
	clientFactory deployments.WorkerClientFactory,
	log zerolog.Logger,
) *Reconciler {
	logger := log.With().Str("component", "reconciler").Logger()
	defaultTimeout := 15 * time.Second
	return &Reconciler{
		registry:      registry,
		depRepo:       depRepo,
		instRepo:      instRepo,
		sched:         sched,
		clientFactory: clientFactory,
		repairService: NewRepairService(registry, depRepo, instRepo, sched, clientFactory, defaultTimeout, logger),
		interval:      5 * time.Second,
		timeout:       defaultTimeout,
		log:           logger,
		stopCh:        make(chan struct{}),
	}
}

// SetInterval configures the periodic reconciliation interval (RCN-01).
func (r *Reconciler) SetInterval(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d > 0 {
		r.interval = d
	}
}

// SetTimeout configures the repair timeout (RCN-02).
func (r *Reconciler) SetTimeout(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d > 0 {
		r.timeout = d
		r.repairService = NewRepairService(r.registry, r.depRepo, r.instRepo, r.sched, r.clientFactory, d, r.log)
	}
}

// SetServiceRegistry registers the discovery ServiceRegistry for endpoint updates (G-10, G-12).
func (r *Reconciler) SetServiceRegistry(sr *discovery.ServiceRegistry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.serviceRegistry = sr
}

// SetRouter registers the loadbalancer router for target updates (§18).
func (r *Reconciler) SetRouter(router *loadbalancer.Router) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.router = router
}

// ReconcileOnce executes a single full convergence pass across all active deployments and workers.
func (r *Reconciler) ReconcileOnce(ctx context.Context) (*ReconcileActions, error) {
	actions := &ReconcileActions{}

	// Apply cycle timeout (RCN-02)
	cycleCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// -------------------------------------------------------------
	// 1. Fetch Desired State from PostgreSQL / Repositories (G-24)
	// -------------------------------------------------------------
	deploymentsList, err := r.depRepo.List(cycleCtx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to list deployments: %w", err)
	}

	activeDeployments := make(map[string]*deployments.Deployment)
	for _, d := range deploymentsList {
		// Only steady-state RUNNING deployments are subject to drift reconciliation (RACE-04).
		// In-flight deployments (QUEUED, BUILDING, SCHEDULING, STARTING) are actively deploying
		// and must not be double-created by an overlapping reconcile pass.
		if d.Status == deployments.StatusRunning && !d.Status.IsInFlight() {
			activeDeployments[d.ID] = d
		}
	}

	allInstances, err := r.instRepo.ListAll(cycleCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to list instances: %w", err)
	}

	desiredInstances := make([]DesiredInstance, 0)
	desiredByDep := make(map[string]int)

	for _, inst := range allInstances {
		dep, isActive := activeDeployments[inst.DeploymentID]
		if !isActive {
			continue
		}
		if inst.Status == "STOPPED" || inst.Status == "FAILED" || inst.Status == "PENDING" || inst.Status == "STARTING" {
			continue
		}

		desiredByDep[dep.ID]++

		desiredInstances = append(desiredInstances, DesiredInstance{
			InstanceID:    inst.ID,
			InstanceKey:   inst.InstanceKey,
			DeploymentID:  dep.ID,
			WorkerID:      inst.WorkerID,
			Image:         dep.Image,
			Env:           dep.Env,
			Labels:        dep.Labels,
			DesiredStatus: string(inst.Status),
		})
	}

	// -------------------------------------------------------------
	// 2. Collect Observed Containers from Registered Workers
	// -------------------------------------------------------------
	allWorkers := r.registry.List()
	observedContainers := make([]ObservedContainer, 0)
	workerClients := make(map[string]deployments.WorkerClient)

	for _, w := range allWorkers {
		// If worker is UNHEALTHY or partitioned, it is dead/disconnected; skip collecting its containers (G-12, G-13)
		if w.Health == workers.HealthUnhealthy || w.Health == workers.HealthUnreachable || w.IsPartitioned {
			r.log.Debug().Str("worker_id", w.ID).Str("health", string(w.Health)).Msg("skipping non-healthy worker from observed container collection")
			continue
		}

		client, err := r.clientFactory.GetClient(cycleCtx, w)
		if err != nil {
			r.log.Warn().Err(err).Str("worker_id", w.ID).Msg("failed to get client for worker during reconcile")
			continue
		}
		workerClients[w.ID] = client
		workerClients[w.WorkerKey] = client

		listResp, err := client.ListContainers(cycleCtx, &proto.ListContainersRequest{})
		if err != nil {
			r.log.Warn().Err(err).Str("worker_id", w.ID).Msg("failed to list containers from worker during reconcile")
			continue
		}

		for _, ctr := range listResp.Containers {
			observedContainers = append(observedContainers, ObservedContainer{
				WorkerID:     w.ID,
				WorkerKey:    w.WorkerKey,
				ContainerID:  ctr.ContainerId,
				InstanceKey:  ctr.InstanceId,
				DeploymentID: ctr.Labels["nebula.deployment_id"],
				Image:        ctr.Image,
				State:        ctr.Status,
				Labels:       ctr.Labels,
				CreatedAt:    time.Unix(ctr.CreatedAt, 0),
			})
		}
	}

	// -------------------------------------------------------------
	// 3. Compute Diff (Desired vs. Observed) (CP-05, PERF-01)
	// -------------------------------------------------------------
	diff := ComputeDiff(desiredInstances, observedContainers)

	// -------------------------------------------------------------
	// 4. Enforce Orphan Policy on Extra Containers (G-03, CP-04, CP-06)
	// -------------------------------------------------------------
	for _, extra := range diff.Extra {
		client := workerClients[extra.WorkerID]
		if client == nil {
			client = workerClients[extra.WorkerKey]
		}
		decision := EnforceOrphanPolicy(cycleCtx, extra, client, r.log)
		switch decision.ActionTaken {
		case "STOPPED":
			actions.StoppedCount++
		case "FLAGGED_ONLY":
			actions.FlaggedCount++
		case "ERROR":
			actions.Errors = append(actions.Errors, decision.Error)
		}
	}

	// -------------------------------------------------------------
	// 5. Repair Missing Instances (G-01, G-11, G-23, DA-03, MAN-DEL-01)
	// -------------------------------------------------------------
	// Track currently running instances per deployment
	runningCountByDep := make(map[string]int)
	for _, match := range diff.Matching {
		runningCountByDep[match.Desired.DeploymentID]++
	}

	for _, missing := range diff.Missing {
		dep := activeDeployments[missing.DeploymentID]
		desiredCount := 1
		if dep != nil && dep.InstanceCount > 0 {
			desiredCount = dep.InstanceCount
		}

		currentlyRunning := runningCountByDep[missing.DeploymentID]

		repairRes, err := r.repairService.RepairMissing(cycleCtx, missing, currentlyRunning, desiredCount)
		if err != nil {
			actions.Errors = append(actions.Errors, err.Error())
		} else if repairRes.Success {
			actions.RecreatedCount++
			runningCountByDep[missing.DeploymentID]++

			// G-10 & G-12: Register replacement instance in service discovery and load balancer
			if r.serviceRegistry != nil {
				targetWorker, _ := r.registry.Get(repairRes.WorkerID)
				addr := ""
				if targetWorker != nil {
					addr = "http://" + targetWorker.Address()
				}
				if err := r.serviceRegistry.RegisterEndpoint(cycleCtx, discovery.Endpoint{
					InstanceID:   missing.InstanceID,
					DeploymentID: missing.DeploymentID,
					ProjectID:    dep.ProjectID,
					WorkerID:     repairRes.WorkerID,
					Address:      addr,
					Status:       discovery.EndpointHealthy,
				}); err != nil {
					r.log.Error().Err(err).
						Str("instance_id", missing.InstanceID).
						Str("project_id", dep.ProjectID).
						Msg("failed to register endpoint in service registry")
				}
			}
		}
	}

	// -------------------------------------------------------------
	// 5b. Prune Excess Replicas beyond Desired Count (§18, G-11)
	// -------------------------------------------------------------
	for depID, runningCount := range runningCountByDep {
		dep := activeDeployments[depID]
		if dep == nil {
			continue
		}
		desiredCount := 1
		if dep.InstanceCount > 0 {
			desiredCount = dep.InstanceCount
		}
		if runningCount > desiredCount {
			excess := runningCount - desiredCount
			stoppedForDep := 0
			for i := len(diff.Matching) - 1; i >= 0 && stoppedForDep < excess; i-- {
				match := diff.Matching[i]
				if match.Desired.DeploymentID == depID {
					client := workerClients[match.Observed.WorkerID]
					if client == nil {
						client = workerClients[match.Observed.WorkerKey]
					}
					if client != nil {
						_, _ = client.StopContainer(cycleCtx, &proto.StopContainerRequest{
							InstanceId:     match.Observed.InstanceKey,
							TimeoutSeconds: 5,
						})
					}
					if inst, gErr := r.instRepo.GetByInstanceKey(cycleCtx, match.Observed.InstanceKey); gErr == nil && inst != nil {
						inst.Status = "STOPPED"
						_ = r.instRepo.Update(cycleCtx, inst)
					} else if inst, gErr := r.instRepo.GetByID(cycleCtx, match.Desired.InstanceID); gErr == nil && inst != nil {
						inst.Status = "STOPPED"
						_ = r.instRepo.Update(cycleCtx, inst)
					}
					if r.serviceRegistry != nil {
						r.serviceRegistry.UnregisterEndpoint(cycleCtx, match.Desired.InstanceID)
					}
					if r.router != nil && dep.ProjectID != "" {
						r.router.UnregisterTarget(dep.ProjectID, match.Desired.InstanceID)
					}
					actions.StoppedCount++
					stoppedForDep++
					runningCountByDep[depID]--
				}
			}
		}
	}

	// -------------------------------------------------------------
	// 6. Workload Migration from DRAINING Workers (DRN-02)
	// -------------------------------------------------------------
	migrated, err := r.ReconcileDraining(cycleCtx)
	if err == nil {
		actions.MigratedCount += migrated
	} else {
		actions.Errors = append(actions.Errors, err.Error())
	}

	if actions.TotalActions() > 0 || actions.FlaggedCount > 0 {
		r.log.Info().
			Int("recreated", actions.RecreatedCount).
			Int("stopped", actions.StoppedCount).
			Int("flagged", actions.FlaggedCount).
			Int("migrated", actions.MigratedCount).
			Msg("reconciliation pass complete with actions taken")
	}

	return actions, nil
}

// Start launches the periodic background reconciliation loop (RCN-01).
func (r *Reconciler) Start(ctx context.Context) {
	go func() {
		// Run initial convergence pass on start
		if _, err := r.ReconcileOnce(ctx); err != nil {
			r.log.Error().Err(err).Msg("initial reconciliation pass failed")
		}

		r.mu.Lock()
		interval := r.interval
		r.mu.Unlock()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				r.log.Info().Msg("reconciler periodic loop stopped by context")
				return
			case <-r.stopCh:
				r.log.Info().Msg("reconciler periodic loop stopped")
				return
			case <-ticker.C:
				if _, err := r.ReconcileOnce(ctx); err != nil {
					r.log.Error().Err(err).Msg("periodic reconciliation pass failed")
				}
			}
		}
	}()
}

// Stop stops the background reconciliation loop cleanly.
func (r *Reconciler) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
}

// ReconcileDraining scans for DRAINING workers and gradually migrates existing workloads
// to other eligible, healthy workers without instant force-killing (DRN-02).
func (r *Reconciler) ReconcileDraining(ctx context.Context) (int, error) {
	allWorkers := r.registry.List()
	migratedTotal := 0

	for _, w := range allWorkers {
		if w.State != workers.StateDraining {
			continue
		}

		instances, err := r.instRepo.ListByWorker(ctx, w.ID)
		if err != nil {
			r.log.Error().Err(err).Str("worker_id", w.ID).Msg("failed to list instances for draining worker")
			continue
		}

		for _, inst := range instances {
			if inst.Status != "RUNNING" {
				continue
			}

			dep, err := r.depRepo.GetByID(ctx, inst.DeploymentID)
			if err != nil {
				r.log.Error().Err(err).Str("deployment_id", inst.DeploymentID).Msg("failed to get deployment for migration")
				continue
			}

			targetWorker, err := r.sched.SelectWorker(ctx, scheduler.WorkloadRequirement{
				DeploymentID:     dep.ID,
				RequiredCapacity: 1,
				RequiredLabels:   dep.Labels,
			})
			if err != nil {
				r.log.Warn().
					Err(err).
					Str("instance_id", inst.ID).
					Str("draining_worker", w.WorkerKey).
					Msg("migration deferred: no feasible replacement worker available yet")
				continue
			}

			targetClient, err := r.clientFactory.GetClient(ctx, targetWorker)
			if err != nil {
				r.log.Error().Err(err).Str("target_worker", targetWorker.WorkerKey).Msg("failed to dial target worker")
				continue
			}

			runResp, err := targetClient.RunContainer(ctx, &proto.RunContainerRequest{
				InstanceId:   inst.InstanceKey,
				DeploymentId: dep.ID,
				Image:        dep.Image,
				Env:          dep.Env,
				Labels:       dep.Labels,
			})
			if err != nil || (runResp != nil && runResp.Error != "") {
				r.log.Error().
					Err(err).
					Str("instance_id", inst.ID).
					Str("target_worker", targetWorker.WorkerKey).
					Msg("failed to start replacement container on new worker")
				continue
			}

			oldContainerID := inst.ContainerID

			inst.WorkerID = targetWorker.ID
			inst.ContainerID = runResp.ContainerId
			if err := r.instRepo.Update(ctx, inst); err != nil {
				r.log.Error().Err(err).Str("instance_id", inst.ID).Msg("failed to update instance record after migration")
			}

			r.registry.UpdateWorkload(targetWorker.WorkerKey, 1)
			r.registry.UpdateWorkload(w.WorkerKey, -1)

			drainingClient, err := r.clientFactory.GetClient(ctx, w)
			if err == nil {
				if _, stopErr := drainingClient.StopContainer(ctx, &proto.StopContainerRequest{
					InstanceId:     inst.InstanceKey,
					TimeoutSeconds: 10,
				}); stopErr != nil {
					r.log.Warn().Err(stopErr).
						Str("instance_key", inst.InstanceKey).
						Str("from_worker", w.WorkerKey).
						Msg("failed to stop old container on draining worker after migration")
				}
			} else {
				r.log.Warn().Err(err).
					Str("from_worker", w.WorkerKey).
					Msg("failed to get client to stop container on draining worker")
			}

			r.log.Info().
				Str("instance_id", inst.ID).
				Str("instance_key", inst.InstanceKey).
				Str("from_worker", w.WorkerKey).
				Str("to_worker", targetWorker.WorkerKey).
				Str("old_container_id", oldContainerID).
				Str("new_container_id", runResp.ContainerId).
				Msg("instance successfully migrated from draining worker")

			migratedTotal++
		}
	}

	return migratedTotal, nil
}
