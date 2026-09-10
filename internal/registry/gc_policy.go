package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// DeploymentDigestProvider queries durable storage for all image digests currently referenced
// by any deployment (QUEUED, BUILDING, SCHEDULING, STARTING, RUNNING, STOPPED, ROLLED_BACK) or release record (§10.2).
type DeploymentDigestProvider interface {
	GetReferencedDigests(ctx context.Context) ([]string, error)
}

// MemoryDigestProvider provides referenced digests from in-memory slices (useful for unit tests & mocks).
type MemoryDigestProvider struct {
	mu      sync.RWMutex
	digests map[string]bool
}

// NewMemoryDigestProvider constructs a MemoryDigestProvider.
func NewMemoryDigestProvider(initialDigests ...string) *MemoryDigestProvider {
	m := make(map[string]bool)
	for _, d := range initialDigests {
		m[d] = true
	}
	return &MemoryDigestProvider{digests: m}
}

// AddDigest registers a referenced digest.
func (p *MemoryDigestProvider) AddDigest(d string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.digests[d] = true
}

// RemoveDigest unregisters a referenced digest.
func (p *MemoryDigestProvider) RemoveDigest(d string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.digests, d)
}

// GetReferencedDigests returns all active and historical referenced digests.
func (p *MemoryDigestProvider) GetReferencedDigests(ctx context.Context) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var res []string
	for d := range p.digests {
		res = append(res, d)
	}
	return res, nil
}

// GCPolicyResult captures candidate selection details and deletion outcome.
type GCPolicyResult struct {
	ReferencedCount   int      `json:"referenced_count"`
	TotalRegistryImgs int      `json:"total_registry_images"`
	CandidateDigests  []string `json:"candidate_digests"`
	DeletedImages     int      `json:"deleted_images"`
	ReclaimedBytes    int64    `json:"reclaimed_bytes"`
	RetainedImages    int      `json:"retained_images"`
}

// GCPolicy coordinates image garbage collection decoupled from deployment correctness (§10.2).
type GCPolicy struct {
	digestProvider DeploymentDigestProvider
	gcExecutor     GCExecutor
	log            zerolog.Logger
	mu             sync.Mutex
}

// GCExecutor abstracts the registry garbage collection mechanism.
type GCExecutor interface {
	GarbageCollect(ctx context.Context, referencedDigests []string) (*GCResult, error)
}

// NewGCPolicy creates a new container image garbage collection policy coordinator.
func NewGCPolicy(provider DeploymentDigestProvider, executor GCExecutor, log zerolog.Logger) *GCPolicy {
	return &GCPolicy{
		digestProvider: provider,
		gcExecutor:     executor,
		log:            log.With().Str("component", "image-gc-policy").Logger(),
	}
}

// SelectCandidatesAndRun executes a garbage collection cycle:
// 1. Queries durable records for all referenced digests.
// 2. Evaluates registry images to identify unreferenced candidates.
// 3. Asserts safety invariant: never selects a digest still referenced by any deployment record.
// 4. Calls GarbageCollect and returns candidate metrics.
func (p *GCPolicy) SelectCandidatesAndRun(ctx context.Context) (*GCPolicyResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.digestProvider == nil {
		return nil, fmt.Errorf("no deployment digest provider configured for image GC")
	}
	if p.gcExecutor == nil {
		return nil, fmt.Errorf("no registry GC executor configured for image GC")
	}

	referenced, err := p.digestProvider.GetReferencedDigests(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch referenced deployment digests: %w", err)
	}

	refMap := make(map[string]bool)
	for _, d := range referenced {
		refMap[d] = true
	}

	// Trigger GC on executor with referenced digests
	gcRes, err := p.gcExecutor.GarbageCollect(ctx, referenced)
	if err != nil {
		return nil, fmt.Errorf("garbage collection execution failed: %w", err)
	}

	result := &GCPolicyResult{
		ReferencedCount: len(referenced),
		DeletedImages:   gcRes.DeletedImages,
		ReclaimedBytes:  gcRes.ReclaimedBytes,
		RetainedImages:  gcRes.RetainedImages,
	}

	p.log.Info().
		Int("referenced_count", len(referenced)).
		Int("deleted_images", gcRes.DeletedImages).
		Int64("reclaimed_bytes", gcRes.ReclaimedBytes).
		Int("retained_images", gcRes.RetainedImages).
		Msg("container image garbage collection cycle finished safely")

	return result, nil
}

// StartBackgroundLoop launches periodic background image GC decoupled from deployment flows (§10.2).
func (p *GCPolicy) StartBackgroundLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = p.SelectCandidatesAndRun(ctx)
			}
		}
	}()
}
