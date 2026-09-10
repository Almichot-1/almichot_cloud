package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/rs/zerolog"
)

var (
	ErrImageNotFound         = errors.New("image not found in registry")
	ErrRegistryUnavailable   = errors.New("registry service unavailable (G-27)")
	ErrUnauthorizedImagePush = errors.New("unauthorized image push: build sandbox cannot push to repository outside its project namespace (§20)")
)

// StoredImage represents an immutable container image in the registry.
type StoredImage struct {
	Tag    string
	Digest string
	Data   []byte
}

// RegistryClient defines operations for pushing, pulling, and querying images.
type RegistryClient interface {
	Push(ctx context.Context, tag string, data []byte, expectedDigest string) (string, error)
	GetDigest(ctx context.Context, tag string) (string, error)
	Pull(ctx context.Context, tag string) ([]byte, string, error)
	HasImage(ctx context.Context, tag string) bool
}

// MemoryRegistry is an in-memory OCI container registry with strict digest verification.
type MemoryRegistry struct {
	mu        sync.RWMutex
	images    map[string]*StoredImage // keyed by tag
	available bool
	log       zerolog.Logger
}

// NewMemoryRegistry creates a new MemoryRegistry.
func NewMemoryRegistry(log zerolog.Logger) *MemoryRegistry {
	return &MemoryRegistry{
		images:    make(map[string]*StoredImage),
		available: true,
		log:       log.With().Str("component", "registry").Logger(),
	}
}

// SetAvailable toggles whether the registry is available or simulated as down (G-27).
func (r *MemoryRegistry) SetAvailable(available bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.available = available
}

// Push uploads an image to the registry, verifies its cryptographic digest, and stores it (BLD-07, BLD-08).
func (r *MemoryRegistry) Push(ctx context.Context, tag string, data []byte, expectedDigest string) (string, error) {
	r.mu.RLock()
	avail := r.available
	r.mu.RUnlock()
	if !avail {
		return "", ErrRegistryUnavailable
	}

	if tag == "" {
		return "", fmt.Errorf("image tag cannot be empty")
	}

	actualDigest := ComputeDigest(data)

	// BLD-08: Verify digest against expected if supplied
	if expectedDigest != "" {
		if err := VerifyDigest(expectedDigest, actualDigest); err != nil {
			r.log.Error().
				Err(err).
				Str("tag", tag).
				Str("expected", expectedDigest).
				Str("actual", actualDigest).
				Msg("digest mismatch rejected")
			return "", err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.images[tag] = &StoredImage{
		Tag:    tag,
		Digest: actualDigest,
		Data:   data,
	}

	r.log.Info().
		Str("tag", tag).
		Str("digest", actualDigest).
		Int("bytes", len(data)).
		Msg("image pushed to registry successfully (digest verified)")

	return actualDigest, nil
}

// GetDigest returns the verified cryptographic digest for an image tag.
func (r *MemoryRegistry) GetDigest(ctx context.Context, tag string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.available {
		return "", ErrRegistryUnavailable
	}

	img, ok := r.images[tag]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrImageNotFound, tag)
	}
	return img.Digest, nil
}

// Pull downloads an image and returns its payload and verified digest.
func (r *MemoryRegistry) Pull(ctx context.Context, tag string) ([]byte, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.available {
		return nil, "", ErrRegistryUnavailable
	}

	img, ok := r.images[tag]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrImageNotFound, tag)
	}
	return img.Data, img.Digest, nil
}

// HasImage returns true if an image tag exists in the registry.
func (r *MemoryRegistry) HasImage(ctx context.Context, tag string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.available {
		return false
	}

	_, ok := r.images[tag]
	return ok
}

var _ RegistryClient = (*MemoryRegistry)(nil)

// AuthorizePush verifies that an image tag belongs to the project's authorized repository namespace (§20).
// Builds are permitted to push to "nebula/<project_id>:*" or "<registry>/nebula/<project_id>:*".
func AuthorizePush(projectID, imageTag string) error {
	if projectID == "" {
		return fmt.Errorf("%w: missing project_id", ErrUnauthorizedImagePush)
	}
	if imageTag == "" {
		return fmt.Errorf("%w: empty image tag", ErrUnauthorizedImagePush)
	}

	repo := imageTag
	// Strip registry host prefix if present
	if idx := strings.Index(repo, "/"); idx != -1 {
		firstPart := repo[:idx]
		if strings.Contains(firstPart, ":") || strings.Contains(firstPart, ".") || firstPart == "localhost" {
			repo = repo[idx+1:]
		}
	}

	cleanRepo := strings.Split(repo, ":")[0]
	expectedPrefix := fmt.Sprintf("nebula/%s", projectID)
	altPrefix := fmt.Sprintf("%s/", projectID)

	if cleanRepo != expectedPrefix && !strings.HasPrefix(cleanRepo, expectedPrefix+"/") &&
		cleanRepo != projectID && !strings.HasPrefix(cleanRepo, altPrefix) {
		return fmt.Errorf("%w: project '%s' not authorized to push to '%s'", ErrUnauthorizedImagePush, projectID, imageTag)
	}
	return nil
}
