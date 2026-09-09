package loadbalancer

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

// Target represents a reachable backend instance endpoint.
type Target struct {
	InstanceID string
	URL        *url.URL
	Proxy      *httputil.ReverseProxy
}

// Router acts as an HTTP reverse proxy / load balancer for deployed instances (OW-01).
type Router struct {
	mu      sync.RWMutex
	targets map[string][]*Target // keyed by project_id
	counter uint64
	log     zerolog.Logger
}

// NewRouter creates a new load balancer Router.
func NewRouter(log zerolog.Logger) *Router {
	return &Router{
		targets: make(map[string][]*Target),
		log:     log.With().Str("component", "load-balancer").Logger(),
	}
}

// RegisterTarget adds or updates a backend target for a project.
func (r *Router) RegisterTarget(projectID, instanceID, targetRawURL string) error {
	if !strings.HasPrefix(targetRawURL, "http://") && !strings.HasPrefix(targetRawURL, "https://") {
		targetRawURL = "http://" + targetRawURL
	}

	targetURL, err := url.Parse(targetRawURL)
	if err != nil {
		return fmt.Errorf("invalid target URL %s: %w", targetRawURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove existing if present
	existing := r.targets[projectID]
	var updated []*Target
	for _, t := range existing {
		if t.InstanceID != instanceID {
			updated = append(updated, t)
		}
	}

	t := &Target{
		InstanceID: instanceID,
		URL:        targetURL,
		Proxy:      proxy,
	}
	updated = append(updated, t)
	r.targets[projectID] = updated

	r.log.Info().
		Str("project_id", projectID).
		Str("instance_id", instanceID).
		Str("target_url", targetRawURL).
		Msg("registered backend target in load balancer")

	return nil
}

// UnregisterTarget removes an instance from the load balancer routing table.
func (r *Router) UnregisterTarget(projectID, instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing := r.targets[projectID]
	var updated []*Target
	for _, t := range existing {
		if t.InstanceID != instanceID {
			updated = append(updated, t)
		}
	}
	r.targets[projectID] = updated
}

// GetTargets returns the list of target URLs for a project.
func (r *Router) GetTargets(projectID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var urls []string
	for _, t := range r.targets[projectID] {
		urls = append(urls, t.URL.String())
	}
	return urls
}

// ServeHTTP routes incoming traffic to healthy running instances of a project (OW-01).
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Determine project ID from header, query param, or path prefix
	projectID := req.Header.Get("X-Project-ID")
	if projectID == "" {
		projectID = req.URL.Query().Get("project")
	}
	if projectID == "" && strings.HasPrefix(req.URL.Path, "/services/") {
		parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/services/"), "/")
		if len(parts) > 0 {
			projectID = parts[0]
		}
	}

	if projectID == "" {
		http.Error(w, "missing project identifier (X-Project-ID header or /services/{project-id} path)", http.StatusBadRequest)
		return
	}

	r.mu.RLock()
	targets := r.targets[projectID]
	r.mu.RUnlock()

	if len(targets) == 0 {
		http.Error(w, fmt.Sprintf("no active instances for project %s", projectID), http.StatusServiceUnavailable)
		return
	}

	// Round-robin selection
	idx := atomic.AddUint64(&r.counter, 1) % uint64(len(targets))
	chosen := targets[idx]

	w.Header().Set("X-Forwarded-By", "nebula-loadbalancer")
	w.Header().Set("X-Routed-Instance", chosen.InstanceID)

	chosen.Proxy.ServeHTTP(w, req)
}
