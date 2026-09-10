package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// EmbeddedRegistryServer is a self-hosted, in-process OCI Distribution Registry (v2) server (§10).
// It implements the standard Docker Registry v2 HTTP API to store and serve container manifests
// and layer blobs, decoupling image creation from multi-node worker execution.
type EmbeddedRegistryServer struct {
	mu         sync.RWMutex
	manifests  map[string][]byte // "repo:tag" or "repo:digest" -> manifest json
	manifestDg map[string]string // "repo:tag" -> "sha256:..."
	blobs      map[string][]byte // digest -> blob content
	uploads    map[string][]byte // upload-id -> pending blob buffer
	listener   net.Listener
	httpServer *http.Server
	available  bool // toggle to simulate registry downtime (G-27)
	log        zerolog.Logger
	addr       string
}

// NewEmbeddedRegistryServer creates a new OCI v2 distribution registry server.
func NewEmbeddedRegistryServer(log zerolog.Logger) *EmbeddedRegistryServer {
	return &EmbeddedRegistryServer{
		manifests:  make(map[string][]byte),
		manifestDg: make(map[string]string),
		blobs:      make(map[string][]byte),
		uploads:    make(map[string][]byte),
		available:  true,
		log:        log.With().Str("component", "embedded-registry").Logger(),
	}
}

// SetAvailable toggles whether the registry is reachable or returns 503 Service Unavailable (G-27).
func (s *EmbeddedRegistryServer) SetAvailable(available bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = available
}

// IsAvailable reports current registry availability.
func (s *EmbeddedRegistryServer) IsAvailable() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.available
}

// Start binds to the given address (e.g. "127.0.0.1:0" for dynamic port) and serves HTTP.
func (s *EmbeddedRegistryServer) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("registry listen: %w", err)
	}

	s.listener = lis
	s.addr = lis.Addr().String()

	s.httpServer = &http.Server{
		Handler: s,
	}

	go func() {
		if err := s.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
			s.log.Error().Err(err).Msg("embedded registry server stopped unexpectedly")
		}
	}()

	s.log.Info().Str("addr", s.addr).Msg("self-hosted OCI distribution registry started")
	return nil
}

// Addr returns the listening host:port address.
func (s *EmbeddedRegistryServer) Addr() string {
	return s.addr
}

// Close gracefully terminates the registry server.
func (s *EmbeddedRegistryServer) Close() error {
	if s.httpServer != nil {
		return s.httpServer.Close()
	}
	return nil
}

// ServeHTTP handles Docker / OCI Distribution v2 API requests.
func (s *EmbeddedRegistryServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	avail := s.available
	s.mu.RUnlock()

	if !avail {
		http.Error(w, `{"errors":[{"code":"UNAVAILABLE","message":"registry service unavailable (G-27)"}]}`, http.StatusServiceUnavailable)
		return
	}

	// Always set Docker registry header
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")

	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	// 1. Version check: GET /v2/
	if path == "v2" || path == "v2/" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
		return
	}

	if len(parts) < 2 || parts[0] != "v2" {
		http.NotFound(w, r)
		return
	}

	// Handle manifests: /v2/<name...>/manifests/<reference>
	if strings.Contains(path, "/manifests/") {
		s.handleManifests(w, r, parts)
		return
	}

	// Handle blobs: /v2/<name...>/blobs/uploads/ or /v2/<name...>/blobs/<digest>
	if strings.Contains(path, "/blobs/") {
		s.handleBlobs(w, r, parts)
		return
	}

	http.NotFound(w, r)
}

func (s *EmbeddedRegistryServer) handleManifests(w http.ResponseWriter, r *http.Request, parts []string) {
	// e.g. ["v2", "myrepo", "manifests", "v1"] or ["v2", "org", "myrepo", "manifests", "v1"]
	manifestIdx := -1
	for i, p := range parts {
		if p == "manifests" {
			manifestIdx = i
			break
		}
	}
	if manifestIdx < 2 || manifestIdx+1 >= len(parts) {
		http.NotFound(w, r)
		return
	}

	repo := strings.Join(parts[1:manifestIdx], "/")
	reference := parts[manifestIdx+1]
	key := fmt.Sprintf("%s:%s", repo, reference)

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		h := sha256.Sum256(body)
		digest := fmt.Sprintf("sha256:%s", hex.EncodeToString(h[:]))

		s.mu.Lock()
		s.manifests[key] = body
		s.manifests[fmt.Sprintf("%s:%s", repo, digest)] = body
		s.manifestDg[key] = digest
		s.mu.Unlock()

		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Location", fmt.Sprintf("/v2/%s/manifests/%s", repo, digest))
		w.WriteHeader(http.StatusCreated)

	case http.MethodGet, http.MethodHead:
		s.mu.RLock()
		body, ok := s.manifests[key]
		digest := s.manifestDg[key]
		if !ok && strings.HasPrefix(reference, "sha256:") {
			body, ok = s.manifests[fmt.Sprintf("%s:%s", repo, reference)]
			digest = reference
		}
		s.mu.RUnlock()

		if !ok {
			http.Error(w, `{"errors":[{"code":"MANIFEST_UNKNOWN","message":"manifest unknown"}]}`, http.StatusNotFound)
			return
		}

		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)

		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *EmbeddedRegistryServer) handleBlobs(w http.ResponseWriter, r *http.Request, parts []string) {
	// Handle /v2/<name>/blobs/uploads/
	if strings.Contains(r.URL.Path, "/blobs/uploads") {
		switch r.Method {
		case http.MethodPost:
			uploadID := uuid.New().String()
			s.mu.Lock()
			s.uploads[uploadID] = nil
			s.mu.Unlock()

			w.Header().Set("Location", fmt.Sprintf("/v2/blobs/uploads/%s", uploadID))
			w.Header().Set("Docker-Upload-UUID", uploadID)
			w.Header().Set("Range", "0-0")
			w.WriteHeader(http.StatusAccepted)

		case http.MethodPut:
			uploadID := parts[len(parts)-1]
			body, _ := io.ReadAll(r.Body)
			defer r.Body.Close()

			h := sha256.Sum256(body)
			digest := fmt.Sprintf("sha256:%s", hex.EncodeToString(h[:]))

			s.mu.Lock()
			s.blobs[digest] = body
			delete(s.uploads, uploadID)
			s.mu.Unlock()

			w.Header().Set("Docker-Content-Digest", digest)
			w.WriteHeader(http.StatusCreated)

		default:
			w.WriteHeader(http.StatusAccepted)
		}
		return
	}

	// Handle /v2/<name>/blobs/<digest>
	digest := parts[len(parts)-1]
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		s.mu.RLock()
		content, ok := s.blobs[digest]
		s.mu.RUnlock()

		if !ok {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
		w.WriteHeader(http.StatusOK)

		if r.Method == http.MethodGet {
			_, _ = w.Write(content)
		}

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
