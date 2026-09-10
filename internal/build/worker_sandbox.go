package build

import (
	"context"
	"fmt"
	"sync"

	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// BuildWorkerClient abstracts execution of a build job on a dedicated Build Worker node.
type BuildWorkerClient interface {
	ExecuteBuild(ctx context.Context, req BuildRequest) (*BuildResult, error)
}

// BuildWorkerClientFactory produces or retrieves BuildWorkerClients for a specific worker node.
type BuildWorkerClientFactory interface {
	GetClient(ctx context.Context, worker *workers.Worker) (BuildWorkerClient, error)
}

// LocalBuildWorkerPool tracks in-process BuildWorkerClients by WorkerKey or Address.
type LocalBuildWorkerPool struct {
	mu      sync.RWMutex
	workers map[string]BuildWorkerClient
}

// NewLocalBuildWorkerPool creates a new LocalBuildWorkerPool.
func NewLocalBuildWorkerPool() *LocalBuildWorkerPool {
	return &LocalBuildWorkerPool{
		workers: make(map[string]BuildWorkerClient),
	}
}

// RegisterWorker registers a BuildWorkerClient under a worker key or address.
func (p *LocalBuildWorkerPool) RegisterWorker(key string, client BuildWorkerClient) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers[key] = client
}

// UnregisterWorker removes a worker from the pool.
func (p *LocalBuildWorkerPool) UnregisterWorker(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.workers, key)
}

// GetClient retrieves the BuildWorkerClient for a scheduled worker.
func (p *LocalBuildWorkerPool) GetClient(ctx context.Context, worker *workers.Worker) (BuildWorkerClient, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if client, ok := p.workers[worker.WorkerKey]; ok {
		return client, nil
	}
	if client, ok := p.workers[worker.ID]; ok {
		return client, nil
	}
	if client, ok := p.workers[worker.Address()]; ok {
		return client, nil
	}

	return nil, fmt.Errorf("no build client registered for worker %s (%s)", worker.WorkerKey, worker.Address())
}

// WorkerSandbox implements Sandbox by selecting a dedicated build worker via
// the Scheduler and dispatching build execution to that worker node (§9.3, Phase 12).
type WorkerSandbox struct {
	sched         *scheduler.Scheduler
	registry      *workers.Registry
	clientFactory BuildWorkerClientFactory
	log           zerolog.Logger
	mu            sync.Mutex
}

// NewWorkerSandbox creates a new WorkerSandbox.
func NewWorkerSandbox(
	sched *scheduler.Scheduler,
	registry *workers.Registry,
	clientFactory BuildWorkerClientFactory,
	log zerolog.Logger,
) *WorkerSandbox {
	return &WorkerSandbox{
		sched:         sched,
		registry:      registry,
		clientFactory: clientFactory,
		log:           log.With().Str("component", "worker-build-sandbox").Logger(),
	}
}

// Build schedules and executes a build on a dedicated build worker.
func (s *WorkerSandbox) Build(ctx context.Context, req BuildRequest) (*BuildResult, error) {
	if s.sched == nil {
		return nil, fmt.Errorf("no scheduler configured for worker build sandbox")
	}

	// Request placement strictly on a build-capable worker (Rule 7, §9.3)
	workloadReq := scheduler.WorkloadRequirement{
		RequiredLabels: map[string]string{
			"capability": workers.CapabilityBuild,
		},
		RequiredCapacity: 1,
	}

	// Atomically select worker and increment workload so concurrent builds spread properly
	s.mu.Lock()
	selectedWorker, err := s.sched.SelectWorker(ctx, workloadReq)
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("failed to schedule build to dedicated build worker: %w", err)
	}

	if s.registry != nil {
		s.registry.UpdateWorkload(selectedWorker.WorkerKey, 1)
	}
	s.mu.Unlock()

	if s.registry != nil {
		defer s.registry.UpdateWorkload(selectedWorker.WorkerKey, -1)
	}

	s.log.Info().
		Str("worker_key", selectedWorker.WorkerKey).
		Str("worker_id", selectedWorker.ID).
		Str("project_id", req.ProjectID).
		Str("image_tag", req.ImageTag).
		Msg("build scheduled to dedicated build worker")

	client, err := s.clientFactory.GetClient(ctx, selectedWorker)
	if err != nil {
		return nil, fmt.Errorf("failed to get build worker client: %w", err)
	}

	result, err := client.ExecuteBuild(ctx, req)
	if err != nil {
		s.log.Warn().
			Err(err).
			Str("worker_key", selectedWorker.WorkerKey).
			Str("image_tag", req.ImageTag).
			Msg("build execution failed on dedicated build worker")
		return nil, err
	}

	s.log.Info().
		Str("worker_key", selectedWorker.WorkerKey).
		Str("image_tag", req.ImageTag).
		Str("digest", result.Digest).
		Msg("build completed successfully on dedicated build worker")

	return result, nil
}
