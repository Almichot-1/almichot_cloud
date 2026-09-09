package workers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// RegisterParams contains inputs when registering a worker.
type RegisterParams struct {
	WorkerKey string
	Hostname  string
	IPAddress string
	GRPCPort  int
	Capacity  int
	Labels    map[string]string
}

// Registry manages the set of registered workers, combining fast in-memory
// caching for scheduling with durable storage persistence.
type Registry struct {
	mu        sync.RWMutex
	workers   map[string]*Worker // indexed by WorkerKey
	byID      map[string]*Worker // indexed by ID
	repo      WorkerRepository
	healthCfg HealthStateMachineConfig
	log       zerolog.Logger
}

// NewRegistry creates a new worker registry.
func NewRegistry(repo WorkerRepository, log zerolog.Logger) *Registry {
	if repo == nil {
		repo = NewMemoryWorkerRepository()
	}

	reg := &Registry{
		workers:   make(map[string]*Worker),
		byID:      make(map[string]*Worker),
		repo:      repo,
		healthCfg: DefaultHealthConfig(),
		log:       log.With().Str("component", "worker-registry").Logger(),
	}


	// Warm in-memory cache from repository if available
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if list, err := repo.List(ctx); err == nil {
		for _, w := range list {
			reg.workers[w.WorkerKey] = w
			if w.ID != "" {
				reg.byID[w.ID] = w
			}
		}
		reg.log.Info().Int("count", len(list)).Msg("loaded workers from repository into cache")
	}

	return reg
}

// Register registers or updates a worker, setting it to READY and Schedulable.
func (r *Registry) Register(ctx context.Context, params RegisterParams) (*Worker, error) {
	if params.WorkerKey == "" {
		return nil, fmt.Errorf("worker_key is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	w, exists := r.workers[params.WorkerKey]
	if !exists {
		w = &Worker{
			ID:          uuid.New().String(),
			WorkerKey:   params.WorkerKey,
			State:       StateReady,
			Health:      HealthHealthy,
			Schedulable: true,
			CreatedAt:   now,
		}
	}

	w.Hostname = params.Hostname
	w.IPAddress = params.IPAddress
	w.GRPCPort = params.GRPCPort
	w.Capacity = params.Capacity
	w.Labels = params.Labels
	w.State = StateReady
	w.Health = HealthHealthy
	w.Schedulable = true
	w.LastBeatAt = &now
	w.UpdatedAt = now

	// Save to durable repository
	if err := r.repo.Upsert(ctx, w); err != nil {
		r.log.Error().Err(err).Str("worker_key", w.WorkerKey).Msg("failed to persist worker to repository")
		// Continue with in-memory cache update
	}

	r.workers[w.WorkerKey] = w
	if w.ID != "" {
		r.byID[w.ID] = w
	}

	r.log.Info().
		Str("worker_key", w.WorkerKey).
		Str("worker_id", w.ID).
		Str("address", w.Address()).
		Int("capacity", w.Capacity).
		Msg("worker registered successfully")

	clone := *w
	return &clone, nil
}

// SetHealthConfig configures health thresholds and hysteresis.
func (r *Registry) SetHealthConfig(cfg HealthStateMachineConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.healthCfg = cfg
}

// GetHealthConfig returns current health state machine configuration.
func (r *Registry) GetHealthConfig() HealthStateMachineConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.healthCfg
}

// Heartbeat records a heartbeat event for the given worker and updates hysteresis.
func (r *Registry) Heartbeat(ctx context.Context, workerIDOrKey string) error {
	return r.HeartbeatAt(ctx, workerIDOrKey, time.Now().UTC())
}

// HeartbeatAt records a heartbeat event at a specific timestamp.
func (r *Registry) HeartbeatAt(ctx context.Context, workerIDOrKey string, at time.Time) error {
	r.mu.Lock()

	w, ok := r.findWorkerLocked(workerIDOrKey)
	if !ok {
		r.mu.Unlock()
		return ErrWorkerNotFound
	}

	w.LastBeatAt = &at
	w.UpdatedAt = at

	// Apply anti-flapping hysteresis
	oldHealth := w.Health
	newHealth := ApplyHeartbeatSuccess(w, r.healthCfg)
	if oldHealth != newHealth {
		r.log.Info().
			Str("worker_key", w.WorkerKey).
			Str("from", string(oldHealth)).
			Str("to", string(newHealth)).
			Int("consecutive_beats", w.ConsecutiveBeats).
			Msg("worker health transitioned on heartbeat (FS-07)")
	}

	workerKey := w.WorkerKey
	r.mu.Unlock()

	_ = r.repo.UpdateHeartbeat(ctx, workerKey, at)
	return nil
}

// SetHealthWithReason explicitly updates a worker's health state with context and reason.
func (r *Registry) SetHealthWithReason(ctx context.Context, workerIDOrKey string, health WorkerHealth, reason string) (*Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.findWorkerLocked(workerIDOrKey)
	if !ok {
		return nil, ErrWorkerNotFound
	}

	oldHealth := w.Health
	w.Health = health
	if health != HealthHealthy {
		w.ConsecutiveBeats = 0
	}
	w.UpdatedAt = time.Now().UTC()

	r.log.Warn().
		Str("worker_key", w.WorkerKey).
		Str("from", string(oldHealth)).
		Str("to", string(health)).
		Str("reason", reason).
		Msg("worker health state changed")

	_ = r.repo.Upsert(ctx, w)
	clone := *w
	return &clone, nil
}

// SetHealth updates the health status of a worker (backward compatible helper).
func (r *Registry) SetHealth(workerIDOrKey string, health WorkerHealth) {
	_, _ = r.SetHealthWithReason(context.Background(), workerIDOrKey, health, "explicit update")
}


// SetPartitioned marks a worker as network partitioned / unreachable from Control Plane (G-13, FS-05).
func (r *Registry) SetPartitioned(ctx context.Context, workerIDOrKey string, partitioned bool) (*Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.findWorkerLocked(workerIDOrKey)
	if !ok {
		return nil, ErrWorkerNotFound
	}

	w.IsPartitioned = partitioned
	if partitioned {
		w.Health = HealthUnreachable
		w.ConsecutiveBeats = 0
	} else if w.Health == HealthUnreachable {
		// When partition healed, return to SUSPECTED until consecutive heartbeats verify health (FS-06, FS-07)
		w.Health = HealthSuspected
	}
	w.UpdatedAt = time.Now().UTC()

	r.log.Warn().
		Str("worker_key", w.WorkerKey).
		Bool("partitioned", partitioned).
		Str("health", string(w.Health)).
		Msg("worker partition state updated (G-13, FS-05)")

	clone := *w
	return &clone, nil
}


// Drain marks a worker as DRAINING and sets Schedulable to false (G-07).
func (r *Registry) Drain(ctx context.Context, workerIDOrKey string) (*Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.findWorkerLocked(workerIDOrKey)
	if !ok {
		return nil, ErrWorkerNotFound
	}

	w.State = StateDraining
	w.Schedulable = false
	w.UpdatedAt = time.Now().UTC()

	_ = r.repo.SetDrain(ctx, w.WorkerKey, true)

	r.log.Info().
		Str("worker_key", w.WorkerKey).
		Str("worker_id", w.ID).
		Msg("worker state changed to DRAINING (excluded from scheduling)")

	clone := *w
	return &clone, nil
}

// Undrain restores a worker from DRAINING back to READY and Schedulable.
func (r *Registry) Undrain(ctx context.Context, workerIDOrKey string) (*Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.findWorkerLocked(workerIDOrKey)
	if !ok {
		return nil, ErrWorkerNotFound
	}

	w.State = StateReady
	w.Schedulable = true
	w.UpdatedAt = time.Now().UTC()

	_ = r.repo.SetDrain(ctx, w.WorkerKey, false)

	clone := *w
	return &clone, nil
}

// Get looks up a worker by its ID or WorkerKey.
func (r *Registry) Get(workerIDOrKey string) (*Worker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	w, ok := r.findWorkerLocked(workerIDOrKey)
	if !ok {
		return nil, false
	}
	clone := *w
	return &clone, true
}

// List returns a snapshot of all registered workers.
func (r *Registry) List() []*Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]*Worker, 0, len(r.workers))
	for _, w := range r.workers {
		clone := *w
		res = append(res, &clone)
	}
	return res
}

// UpdateWorkload adjusts the active workload count for a worker.
func (r *Registry) UpdateWorkload(workerIDOrKey string, delta int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.findWorkerLocked(workerIDOrKey)
	if !ok {
		return
	}

	w.ActiveWorkloads += delta
	if w.ActiveWorkloads < 0 {
		w.ActiveWorkloads = 0
	}
}

func (r *Registry) findWorkerLocked(keyOrID string) (*Worker, bool) {
	if w, ok := r.workers[keyOrID]; ok {
		return w, true
	}
	if w, ok := r.byID[keyOrID]; ok {
		return w, true
	}
	return nil, false
}
