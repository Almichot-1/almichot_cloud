package api

import (
	"net/http"

	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
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
	auth        auth.Authenticator
	secretStore secrets.SecretStore
	redactor    *secrets.Redactor
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
		log:         log.With().Str("component", "http-api").Logger(),
		svc:         svc,
		projects:    projectsRepo,
		registry:    registry,
		router:      router,
		idempotency: NewIdempotencyStore(),
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

// NewRouter builds the full HTTP handler tree with middleware applied.
func NewRouter(s *Server) http.Handler {
	mux := http.NewServeMux()

	// Projects
	mux.HandleFunc("POST /v1/projects", s.createProject)
	mux.HandleFunc("GET /v1/projects", s.listProjects)
	mux.HandleFunc("GET /v1/projects/{id}", s.getProject)

	// Deployments
	mux.HandleFunc("POST /v1/projects/{projectID}/deployments", s.createDeployment)
	mux.HandleFunc("GET /v1/projects/{projectID}/deployments", s.listDeployments)
	mux.HandleFunc("GET /v1/deployments/{id}", s.getDeployment)

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

	// Load balancer reverse proxy: routes /services/{project}/* to the running instances.
	if s.router != nil {
		mux.Handle("/services/", s.router)
	}

	handler := http.Handler(mux)

	// Apply authentication middleware if configured (SE-01, Gate G-25)
	if s.auth != nil {
		handler = auth.AuthMiddleware(s.auth)(handler)
	}

	handler = requestLogger(s.log)(handler)
	handler = requestID(handler)
	handler = recoverer(handler)
	return handler
}