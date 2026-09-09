package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/rs/zerolog"
)

// ServiceRegistry tracks active network endpoints and coordinates routing with the Load Balancer (G-10, SD-05, LB-01).
type ServiceRegistry struct {
	mu        sync.RWMutex
	endpoints map[string]*Endpoint            // InstanceID -> Endpoint
	byWorker  map[string]map[string]*Endpoint // WorkerID -> InstanceID -> Endpoint
	byProject map[string]map[string]*Endpoint // ProjectID -> InstanceID -> Endpoint
	router    *loadbalancer.Router
	log       zerolog.Logger
}

// NewServiceRegistry creates a new ServiceRegistry.
func NewServiceRegistry(router *loadbalancer.Router, log zerolog.Logger) *ServiceRegistry {
	return &ServiceRegistry{
		endpoints: make(map[string]*Endpoint),
		byWorker:  make(map[string]map[string]*Endpoint),
		byProject: make(map[string]map[string]*Endpoint),
		router:    router,
		log:       log.With().Str("component", "service-discovery").Logger(),
	}
}

// RegisterEndpoint registers or updates a service endpoint and adds it to the load balancer if healthy.
func (r *ServiceRegistry) RegisterEndpoint(ctx context.Context, ep Endpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	if ep.UpdatedAt.IsZero() {
		ep.UpdatedAt = now
	}

	clone := ep
	r.endpoints[ep.InstanceID] = &clone

	if r.byWorker[ep.WorkerID] == nil {
		r.byWorker[ep.WorkerID] = make(map[string]*Endpoint)
	}
	r.byWorker[ep.WorkerID][ep.InstanceID] = &clone

	if r.byProject[ep.ProjectID] == nil {
		r.byProject[ep.ProjectID] = make(map[string]*Endpoint)
	}
	r.byProject[ep.ProjectID][ep.InstanceID] = &clone

	// Route traffic only if healthy (G-10)
	if ep.Status == EndpointHealthy && r.router != nil && ep.Address != "" {
		if err := r.router.RegisterTarget(ep.ProjectID, ep.InstanceID, ep.Address); err != nil {
			r.log.Error().Err(err).Str("instance_id", ep.InstanceID).Msg("failed to register target in router")
			return err
		}
	}

	r.log.Info().
		Str("instance_id", ep.InstanceID).
		Str("project_id", ep.ProjectID).
		Str("worker_id", ep.WorkerID).
		Str("address", ep.Address).
		Str("status", string(ep.Status)).
		Msg("service endpoint registered")

	return nil
}

// UnregisterEndpoint removes an endpoint completely from service discovery and the load balancer.
func (r *ServiceRegistry) UnregisterEndpoint(ctx context.Context, instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep, ok := r.endpoints[instanceID]
	if !ok {
		return
	}

	delete(r.endpoints, instanceID)
	if r.byWorker[ep.WorkerID] != nil {
		delete(r.byWorker[ep.WorkerID], instanceID)
	}
	if r.byProject[ep.ProjectID] != nil {
		delete(r.byProject[ep.ProjectID], instanceID)
	}

	if r.router != nil {
		r.router.UnregisterTarget(ep.ProjectID, instanceID)
	}

	r.log.Info().
		Str("instance_id", instanceID).
		Str("project_id", ep.ProjectID).
		Msg("service endpoint unregistered")
}

// SetEndpointHealth updates health status of an endpoint.
// If UNHEALTHY, it is removed from service discovery active routing and load balancer (SD-05, LB-01, G-10).
func (r *ServiceRegistry) SetEndpointHealth(ctx context.Context, instanceID string, healthy bool) (*Endpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep, ok := r.endpoints[instanceID]
	if !ok {
		return nil, fmt.Errorf("endpoint for instance %s not found", instanceID)
	}

	oldStatus := ep.Status
	if healthy {
		ep.Status = EndpointHealthy
	} else {
		ep.Status = EndpointUnhealthy
	}
	ep.UpdatedAt = time.Now().UTC()

	// G-10 / LB-01 / SD-05: Unhealthy endpoints pulled from load-balancer routing
	if r.router != nil {
		if !healthy {
			r.router.UnregisterTarget(ep.ProjectID, ep.InstanceID)
			r.log.Warn().
				Str("instance_id", instanceID).
				Str("project_id", ep.ProjectID).
				Msg("unhealthy endpoint pulled from load-balancer routing (SD-05, LB-01, G-10)")
		} else if oldStatus != EndpointHealthy && ep.Address != "" {
			_ = r.router.RegisterTarget(ep.ProjectID, ep.InstanceID, ep.Address)
			r.log.Info().
				Str("instance_id", instanceID).
				Str("project_id", ep.ProjectID).
				Msg("healthy endpoint restored to load-balancer routing")
		}
	}

	clone := *ep
	return &clone, nil
}

// EvictWorkerEndpoints disables and unregisters all endpoints hosted on an unhealthy/dead worker (G-10, G-12).
func (r *ServiceRegistry) EvictWorkerEndpoints(ctx context.Context, workerID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	instances, ok := r.byWorker[workerID]
	if !ok {
		return nil
	}

	var evicted []string
	for instID, ep := range instances {
		ep.Status = EndpointUnhealthy
		ep.UpdatedAt = time.Now().UTC()

		if r.router != nil {
			r.router.UnregisterTarget(ep.ProjectID, ep.InstanceID)
		}
		evicted = append(evicted, instID)
	}

	r.log.Warn().
		Str("worker_id", workerID).
		Int("evicted_count", len(evicted)).
		Msg("evicted all endpoints on dead/unhealthy worker from service discovery and routing (G-10, G-12)")

	return evicted
}

// GetHealthyEndpoints returns all currently healthy endpoints for a project (SD-05).
func (r *ServiceRegistry) GetHealthyEndpoints(projectID string) []Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []Endpoint
	instances := r.byProject[projectID]
	for _, ep := range instances {
		if ep.Status == EndpointHealthy {
			result = append(result, *ep)
		}
	}
	return result
}

// GetAllEndpoints returns all tracked endpoints.
func (r *ServiceRegistry) GetAllEndpoints() []Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]Endpoint, 0, len(r.endpoints))
	for _, ep := range r.endpoints {
		result = append(result, *ep)
	}
	return result
}
