package api

import (
	"context"
	"net/http"

	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/autoscaler"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/observability"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/secrets"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// Server holds the dependencies of the HTTP control plane API.
type Server struct {
	log         zerolog.Logger
	svc         *deployments.Service
	projects    projects.ProjectRepository
	registry    *workers.Registry
	router      *loadbalancer.Router
	idempotency *IdempotencyStore
	auth         auth.Authenticator
	secretStore  secrets.SecretStore
	redactor     *secrets.Redactor
	autoscaler      *autoscaler.Autoscaler
	enforceHTTPS    bool
	metricsRegistry *observability.Registry
	auditRepo       observability.AuditRepository
	rateLimiter     *loadbalancer.TenantRateLimiter
	enableWAF       bool
}

// NewServer creates the HTTP control plane Server.
func NewServer(
	svc *deployments.Service,
	projectsRepo projects.ProjectRepository,
	registry *workers.Registry,
	router *loadbalancer.Router,
	log zerolog.Logger,
) *Server {
	return &Server{
		log:             log.With().Str("component", "http-api").Logger(),
		svc:             svc,
		projects:        projectsRepo,
		registry:        registry,
		router:          router,
		idempotency:     NewIdempotencyStore(),
		metricsRegistry: observability.DefaultRegistry,
	}
}

// SetAuthenticator configures the request authenticator for protected endpoints (SE-01, G-25).
func (s *Server) SetAuthenticator(a auth.Authenticator) {
	s.auth = a
}

// SetSecretStore sets the secret store for project and deployment secret management.
func (s *Server) SetSecretStore(ss secrets.SecretStore) {
	s.secretStore = ss
}

// SetRedactor sets the redactor for masking sensitive strings from logs.
func (s *Server) SetRedactor(r *secrets.Redactor) {
	s.redactor = r
}

// SetAutoscaler configures the autoscaler instance (§18).
func (s *Server) SetAutoscaler(a *autoscaler.Autoscaler) {
	s.autoscaler = a
}

// SetEnforceHTTPS configures whether HTTPS redirection and strict checking are enforced (§21.1, Phase 13).
func (s *Server) SetEnforceHTTPS(enforce bool) {
	s.enforceHTTPS = enforce
}

// SetMetricsRegistry configures the Prometheus metrics registry (§22.1, Phase 14).
func (s *Server) SetMetricsRegistry(r *observability.Registry) {
	s.metricsRegistry = r
}

// SetAuditRepository sets the append-only, tamper-evident audit log repository (§22, Phase 14).
func (s *Server) SetAuditRepository(repo observability.AuditRepository) {
	s.auditRepo = repo
}

// AuditRepo returns the configured audit repository.
func (s *Server) AuditRepo() observability.AuditRepository {
	return s.auditRepo
}

// SetRateLimiter configures the per-tenant ingress rate limiter (Phase 15, Gate G-42).
func (s *Server) SetRateLimiter(rl *loadbalancer.TenantRateLimiter) {
	s.rateLimiter = rl
}

// SetEnableWAF toggles basic WAF rules inspection (Phase 15).
func (s *Server) SetEnableWAF(enable bool) {
	s.enableWAF = enable
}

// RecordAudit safely records an audit event if audit repository is configured.
func (s *Server) RecordAudit(ctx context.Context, eventType, actor, resource string, details map[string]string) {
	if s.auditRepo != nil {
		_, _ = s.auditRepo.Append(ctx, eventType, actor, resource, details)
	}
}

func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	reg := s.metricsRegistry
	if reg == nil {
		reg = observability.DefaultRegistry
	}
	reg.HTTPHandler()(w, r)
}

// NewRouter builds the full HTTP handler tree with middleware applied.
func NewRouter(s *Server) http.Handler {
	mux := http.NewServeMux()

	// Projects
	mux.HandleFunc("POST /v1/projects", s.createProject)
	mux.HandleFunc("GET /v1/projects", s.listProjects)
	mux.HandleFunc("GET /v1/projects/{id}", s.getProject)
	mux.HandleFunc("POST /v1/projects/{projectID}/scale", s.scaleProject)

	// Deployments
	mux.HandleFunc("POST /v1/projects/{projectID}/deployments", s.createDeployment)
	mux.HandleFunc("GET /v1/projects/{projectID}/deployments", s.listDeployments)
	mux.HandleFunc("GET /v1/deployments/{id}", s.getDeployment)
	mux.HandleFunc("POST /v1/deployments/{id}/rollback", s.rollbackDeployment)
	mux.HandleFunc("POST /v1/deployments/{id}/stop", s.stopDeployment)
	mux.HandleFunc("POST /v1/deployments/{id}/scale", s.scaleDeployment)

	// Autoscaler (§18)
	mux.HandleFunc("POST /v1/projects/{projectID}/autoscaler/policy", s.setAutoscalerPolicy)
	mux.HandleFunc("GET /v1/projects/{projectID}/autoscaler/policy", s.getAutoscalerPolicy)
	mux.HandleFunc("POST /v1/projects/{projectID}/autoscaler/evaluate", s.evaluateAutoscaler)

	// Events & Logs (§19.1, §22, §27.2, Phase 17)
	mux.HandleFunc("GET /v1/projects/{projectID}/events", s.listProjectEvents)
	mux.HandleFunc("GET /v1/deployments/{id}/events", s.listDeploymentEvents)
	mux.HandleFunc("GET /v1/projects/{projectID}/logs", s.listProjectLogs)
	mux.HandleFunc("GET /v1/deployments/{id}/logs", s.listDeploymentLogs)

	// Webhooks (HMAC-SHA256 signature verified)
	mux.HandleFunc("POST /v1/projects/{projectID}/webhooks", s.handleProjectWebhook)

	// Secrets management (encrypted at rest)
	mux.HandleFunc("POST /v1/projects/{projectID}/secrets", s.createSecret)
	mux.HandleFunc("GET /v1/projects/{projectID}/secrets", s.listSecrets)
	mux.HandleFunc("POST /v1/projects/{projectID}/secrets/{name}/rotate", s.rotateSecret)

	// Workers
	mux.HandleFunc("GET /v1/workers", s.listWorkers)

	// Health check (always public)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// Prometheus metrics exposition endpoint (§22.1, Gate G-38)
	mux.HandleFunc("GET /metrics", s.metricsHandler)

	// Load balancer reverse proxy: routes /services/{project}/* to the running instances.
	if s.router != nil {
		mux.Handle("/services/", s.router)
	}

	handler := http.Handler(mux)

	// Ingress hostname routing hook (Phase 15, Gate G-41):
	// If incoming Host is registered to a project in load balancer router, route to backend directly.
	if s.router != nil {
		orig := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			host := r.Host
			if _, ok := s.router.GetProjectForHostname(host); ok {
				s.router.ServeHTTP(w, r)
				return
			}
			orig.ServeHTTP(w, r)
		})
	}

	// Apply authentication middleware if configured (SE-01, Gate G-25)
	if s.auth != nil {
		handler = auth.AuthMiddleware(s.auth)(handler)
	}

	// Apply WAF rules if enabled (Phase 15)
	if s.enableWAF {
		handler = loadbalancer.WAFMiddleware(handler)
	}

	// Apply per-tenant rate limiter if configured (Phase 15, Gate G-42)
	if s.rateLimiter != nil {
		handler = s.rateLimiter.Middleware(handler)
	}

	handler = ClientVersionCheck(1)(handler)
	handler = requestLogger(s.log)(handler)
	handler = requestID(handler)
	handler = recoverer(handler)
	handler = RequireHTTPS(s.enforceHTTPS)(handler)
	return handler
}