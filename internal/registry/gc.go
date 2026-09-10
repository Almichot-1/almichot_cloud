package registry

import (
	"context"
)

// GCResult reports statistics from an image garbage collection cycle (§15).
type GCResult struct {
	DeletedImages  int
	DeletedBlobs   int
	ReclaimedBytes int64
	RetainedImages int
}

// GarbageCollect removes unreferenced images from the in-memory registry,
// guaranteeing that active deployment digests are never touched (§15).
func (r *MemoryRegistry) GarbageCollect(ctx context.Context, referencedDigests []string) (*GCResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	active := make(map[string]bool)
	for _, d := range referencedDigests {
		active[d] = true
	}

	result := &GCResult{}
	for tag, img := range r.images {
		if !active[img.Digest] {
			result.DeletedImages++
			result.ReclaimedBytes += int64(len(img.Data))
			delete(r.images, tag)
		} else {
			result.RetainedImages++
		}
	}

	r.log.Info().
		Int("deleted", result.DeletedImages).
		Int("retained", result.RetainedImages).
		Int64("reclaimed_bytes", result.ReclaimedBytes).
		Msg("registry garbage collection completed")

	return result, nil
}

// GarbageCollect removes unreferenced manifests and blobs from the OCI server (§15).
func (s *EmbeddedRegistryServer) GarbageCollect(ctx context.Context, referencedDigests []string) (*GCResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	active := make(map[string]bool)
	for _, d := range referencedDigests {
		active[d] = true
	}

	result := &GCResult{}

	// Purge unreferenced manifests
	for ref, dg := range s.manifestDg {
		if !active[dg] {
			delete(s.manifestDg, ref)
			delete(s.manifests, ref)
			result.DeletedImages++
		} else {
			result.RetainedImages++
		}
	}

	// Purge unreferenced blobs
	for dg, blob := range s.blobs {
		if !active[dg] {
			result.DeletedBlobs++
			result.ReclaimedBytes += int64(len(blob))
			delete(s.blobs, dg)
		}
	}

	return result, nil
}
