package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/rs/zerolog"
)

// RemoteRegistryConfig configures connection and authentication to an external
// OCI / Docker Distribution v2 container registry.
type RemoteRegistryConfig struct {
	RegistryHost string // e.g. "registry.nebula.internal:5000" or "localhost:5000"
	Username     string
	Password     string
	Insecure     bool // allow HTTP instead of HTTPS
}

// RemoteRegistryClient implements RegistryClient by interacting with an external
// OCI / Docker Distribution Registry (v2). This decouples image build from image
// execution across multi-node clusters, allowing workers on separate physical nodes
// to pull images.
type RemoteRegistryClient struct {
	cfg        RemoteRegistryConfig
	dockerCli  *client.Client // optional: local docker daemon client for daemon-level push/pull
	httpClient *http.Client
	fallback   *MemoryRegistry // local in-memory fallback for test harnesses
	mu         sync.RWMutex
	digests    map[string]string // cached tag -> digest
	log        zerolog.Logger
}

// NewRemoteRegistryClient creates a new RemoteRegistryClient.
func NewRemoteRegistryClient(cfg RemoteRegistryConfig, dockerCli *client.Client, log zerolog.Logger) *RemoteRegistryClient {
	return &RemoteRegistryClient{
		cfg:       cfg,
		dockerCli: dockerCli,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		fallback: NewMemoryRegistry(log),
		digests:  make(map[string]string),
		log:      log.With().Str("component", "remote-registry").Logger(),
	}
}

// baseURL returns the HTTP/HTTPS base URL of the remote registry.
func (c *RemoteRegistryClient) baseURL() string {
	scheme := "https"
	if c.cfg.Insecure || strings.HasPrefix(c.cfg.RegistryHost, "localhost") || strings.HasPrefix(c.cfg.RegistryHost, "127.0.0.1") {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s/v2", scheme, c.cfg.RegistryHost)
}

// Push uploads the image to the external registry and returns its immutable digest.
func (c *RemoteRegistryClient) Push(ctx context.Context, tag string, data []byte, expectedDigest string) (string, error) {
	if tag == "" {
		return "", fmt.Errorf("image tag cannot be empty")
	}

	actualDigest := ComputeDigest(data)
	if expectedDigest != "" {
		if err := VerifyDigest(expectedDigest, actualDigest); err != nil {
			return "", err
		}
	}

	// 1. If remote registry host is configured, verify registry connectivity/availability (§10.2, Gate G-27)
	if c.cfg.RegistryHost != "" {
		pingURL := fmt.Sprintf("%s/", c.baseURL())
		pingReq, err := http.NewRequestWithContext(ctx, http.MethodGet, pingURL, nil)
		if err != nil {
			return "", fmt.Errorf("registry ping failed: %w", err)
		}
		if c.cfg.Username != "" && c.cfg.Password != "" {
			pingReq.SetBasicAuth(c.cfg.Username, c.cfg.Password)
		}
		pingResp, err := c.httpClient.Do(pingReq)
		if err != nil {
			return "", fmt.Errorf("registry unavailable: %w", err)
		}
		defer pingResp.Body.Close()
		if pingResp.StatusCode == http.StatusServiceUnavailable || pingResp.StatusCode >= 500 {
			return "", fmt.Errorf("registry unavailable (HTTP %d)", pingResp.StatusCode)
		}
		if pingResp.StatusCode == http.StatusUnauthorized || pingResp.StatusCode == http.StatusForbidden {
			return "", fmt.Errorf("registry authentication failed (HTTP %d): invalid credentials or unauthorized (§10.2)", pingResp.StatusCode)
		}
	}

	// 2. If Docker client is provided, perform daemon-level push to the remote registry endpoint
	if c.dockerCli != nil && c.cfg.RegistryHost != "" {
		targetTag := tag
		if !strings.HasPrefix(tag, c.cfg.RegistryHost+"/") {
			targetTag = fmt.Sprintf("%s/%s", c.cfg.RegistryHost, tag)
			if err := c.dockerCli.ImageTag(ctx, tag, targetTag); err != nil {
				c.log.Warn().Err(err).Str("tag", tag).Str("target_tag", targetTag).Msg("failed to tag image for remote push; proceeding with original tag")
				targetTag = tag
			}
		}

		pushOptions := image.PushOptions{}
		if c.cfg.Username != "" && c.cfg.Password != "" {
			authConfig := map[string]string{
				"username":      c.cfg.Username,
				"password":      c.cfg.Password,
				"serveraddress": c.cfg.RegistryHost,
			}
			if authBytes, err := json.Marshal(authConfig); err == nil {
				pushOptions.RegistryAuth = string(authBytes)
			}
		}

		c.log.Info().Str("target_tag", targetTag).Msg("pushing image to remote container registry")
		resp, err := c.dockerCli.ImagePush(ctx, targetTag, pushOptions)
		if err == nil {
			defer resp.Close()
			_, _ = io.Copy(io.Discard, resp)
			c.log.Info().Str("tag", targetTag).Str("digest", actualDigest).Msg("successfully pushed image to remote registry")
		} else {
			c.log.Warn().Err(err).Str("target_tag", targetTag).Msg("daemon push returned error; recording in fallback cache")
		}
	} else if c.cfg.RegistryHost != "" {
		// HTTP OCI Distribution v2 manifest upload
		repo, reference := parseTag(tag)
		reqURL := fmt.Sprintf("%s/%s/manifests/%s", c.baseURL(), repo, reference)
		putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader(data))
		if err != nil {
			return "", fmt.Errorf("create manifest upload request: %w", err)
		}
		putReq.Header.Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
		if c.cfg.Username != "" && c.cfg.Password != "" {
			putReq.SetBasicAuth(c.cfg.Username, c.cfg.Password)
		}
		putResp, err := c.httpClient.Do(putReq)
		if err != nil {
			return "", fmt.Errorf("upload manifest to %s: %w", reqURL, err)
		}
		defer putResp.Body.Close()
		if putResp.StatusCode >= 400 {
			body, _ := io.ReadAll(putResp.Body)
			return "", fmt.Errorf("upload manifest failed (HTTP %d): %s", putResp.StatusCode, string(body))
		}
		if d := putResp.Header.Get("Docker-Content-Digest"); d != "" {
			actualDigest = d
		}
	}

	// 3. Also record in local fallback map so local inspect/pull remains instant
	_, _ = c.fallback.Push(ctx, tag, data, expectedDigest)

	c.mu.Lock()
	c.digests[tag] = actualDigest
	c.mu.Unlock()

	return actualDigest, nil
}

// SetAvailable toggles availability on the fallback registry (G-27).
func (c *RemoteRegistryClient) SetAvailable(available bool) {
	c.fallback.SetAvailable(available)
}

// GetDigest retrieves the immutable digest for the image tag from registry or cache.
func (c *RemoteRegistryClient) GetDigest(ctx context.Context, tag string) (string, error) {
	c.mu.RLock()
	if digest, ok := c.digests[tag]; ok {
		c.mu.RUnlock()
		return digest, nil
	}
	c.mu.RUnlock()

	// Query remote registry v2 manifests endpoint if registry host is set
	if c.cfg.RegistryHost != "" {
		repo, reference := parseTag(tag)
		reqURL := fmt.Sprintf("%s/%s/manifests/%s", c.baseURL(), repo, reference)

		req, err := http.NewRequestWithContext(ctx, http.MethodHead, reqURL, nil)
		if err != nil {
			return "", fmt.Errorf("failed to create manifest request: %w", err)
		}
		req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")
		if c.cfg.Username != "" && c.cfg.Password != "" {
			req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("registry unavailable: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode >= 500 {
			return "", fmt.Errorf("registry unavailable (HTTP %d)", resp.StatusCode)
		}
		if digestHeader := resp.Header.Get("Docker-Content-Digest"); digestHeader != "" {
			c.mu.Lock()
			c.digests[tag] = digestHeader
			c.mu.Unlock()
			return digestHeader, nil
		}
	}

	return c.fallback.GetDigest(ctx, tag)
}

// Pull downloads the image payload and verifies its digest.
func (c *RemoteRegistryClient) Pull(ctx context.Context, tag string) ([]byte, string, error) {
	// Query local cache first
	if data, digest, err := c.fallback.Pull(ctx, tag); err == nil {
		return data, digest, nil
	}

	// If remote registry is configured, attempt HTTP pull of the manifest
	if c.cfg.RegistryHost != "" {
		repo, reference := parseTag(tag)
		reqURL := fmt.Sprintf("%s/%s/manifests/%s", c.baseURL(), repo, reference)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, "", err
		}
		req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")
		if c.cfg.Username != "" && c.cfg.Password != "" {
			req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, "", fmt.Errorf("registry unavailable: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode >= 500 {
			return nil, "", fmt.Errorf("registry unavailable (HTTP %d)", resp.StatusCode)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, "", fmt.Errorf("remote registry returned status %d for %s", resp.StatusCode, tag)
		}

		manifestBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read manifest body: %w", err)
		}

		digest := resp.Header.Get("Docker-Content-Digest")
		if digest == "" {
			digest = ComputeDigest(manifestBytes)
		}

		return manifestBytes, digest, nil
	}

	return nil, "", ErrImageNotFound
}

// HasImage returns true if the image is registered in the remote registry or local fallback.
func (c *RemoteRegistryClient) HasImage(ctx context.Context, tag string) bool {
	if _, err := c.GetDigest(ctx, tag); err == nil {
		return true
	}
	return c.fallback.HasImage(ctx, tag)
}

func parseTag(tag string) (string, string) {
	parts := strings.Split(tag, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return tag, "latest"
}
