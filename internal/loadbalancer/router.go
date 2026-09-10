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

// Router acts as an HTTP reverse proxy / load balancer for deployed instances (OW-01, Phase 15).
type Router struct {
	mu           sync.RWMutex
	targets      map[string][]*Target // keyed by project_id
	hostnames    map[string]string    // hostname -> project_id (e.g. myapp.nebula.example -> proj-1)
	cpCallbacks  uint64               // invariant assertion counter: should remain 0 during proxying
	counter      uint64
	log          zerolog.Logger
}

// NewRouter creates a new load balancer Router.
func NewRouter(log zerolog.Logger) *Router {
	return &Router{
		targets:   make(map[string][]*Target),
		hostnames: make(map[string]string),
		log:       log.With().Str("component", "load-balancer").Logger(),
	}
}

// RegisterHostname associates a fully-qualified domain name / hostname with a project.
func (r *Router) RegisterHostname(hostname, projectID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hostname = strings.ToLower(strings.TrimSpace(hostname))
	if strings.Contains(hostname, ":") {
		hostname = strings.Split(hostname, ":")[0]
	}
	r.hostnames[hostname] = projectID
	r.log.Info().Str("hostname", hostname).Str("project_id", projectID).Msg("registered hostname route")
}

// UnregisterHostname removes a hostname route.
func (r *Router) UnregisterHostname(hostname string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hostname = strings.ToLower(strings.TrimSpace(hostname))
	if strings.Contains(hostname, ":") {
		hostname = strings.Split(hostname, ":")[0]
	}
	delete(r.hostnames, hostname)
}

// GetProjectForHostname returns the project ID mapped to a hostname, if any.
func (r *Router) GetProjectForHostname(hostname string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	hostname = strings.ToLower(strings.TrimSpace(hostname))
	if strings.Contains(hostname, ":") {
		hostname = strings.Split(hostname, ":")[0]
	}
	pid, ok := r.hostnames[hostname]
	return pid, ok
}

// CPCallbackCount returns the number of times the load balancer called back into the control plane.
func (r *Router) CPCallbackCount() uint64 {
	return atomic.LoadUint64(&r.cpCallbacks)
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

// ServeHTTP routes incoming traffic to healthy running instances of a project (OW-01, Phase 15).
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 1. Check stable hostname mapping first (Phase 15, Gate G-41)
	var projectID string
	host := req.Host
	if strings.Contains(host, ":") {
		host = strings.Split(host, ":")[0]
	}
	r.mu.RLock()
	if pid, ok := r.hostnames[strings.ToLower(host)]; ok {
		projectID = pid
	}
	r.mu.RUnlock()

	// 2. Fall back to existing path-based routing, header, or query param
	if projectID == "" {
		projectID = req.Header.Get("X-Project-ID")
	}
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
		http.Error(w, "missing project identifier (Host header, X-Project-ID header, or /services/{project-id} path)", http.StatusBadRequest)
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
