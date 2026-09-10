package registry_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/registry"
	"github.com/rs/zerolog"
)

func TestGCPolicy_CandidateSelection_PreservesReferencedDigests(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	reg := registry.NewMemoryRegistry(log)

	// Push 3 images to the registry
	data1 := []byte("image-v1-active")
	d1 := registry.ComputeDigest(data1)
	_, _ = reg.Push(ctx, "app:v1", data1, d1)

	data2 := []byte("image-v2-historical-rollback-target")
	d2 := registry.ComputeDigest(data2)
	_, _ = reg.Push(ctx, "app:v2", data2, d2)

	data3 := []byte("image-v3-orphaned-unreferenced")
	d3 := registry.ComputeDigest(data3)
	_, _ = reg.Push(ctx, "app:v3", data3, d3)

	// Digest provider reports d1 (running deployment) and d2 (rollback release) as referenced
	provider := registry.NewMemoryDigestProvider(d1, d2)

	policy := registry.NewGCPolicy(provider, reg, log)

	// Run GC cycle
	res, err := policy.SelectCandidatesAndRun(ctx)
	if err != nil {
		t.Fatalf("GC policy run failed: %v", err)
	}

	// Assertions:
	// 1. Exactly 1 unreferenced image (d3) deleted
	if res.DeletedImages != 1 {
		t.Fatalf("Expected 1 deleted image, got %d", res.DeletedImages)
	}
	// 2. Exactly 2 referenced images retained
	if res.RetainedImages != 2 {
		t.Fatalf("Expected 2 retained images, got %d", res.RetainedImages)
	}

	// 3. Confirm d1 and d2 are still in registry
	_, pulledD1, err := reg.Pull(ctx, "app:v1")
	if err != nil || pulledD1 != d1 {
		t.Fatalf("active image v1 was unexpectedly purged: %v", err)
	}
	_, pulledD2, err := reg.Pull(ctx, "app:v2")
	if err != nil || pulledD2 != d2 {
		t.Fatalf("rollback release v2 was unexpectedly purged: %v", err)
	}

	// 4. Confirm d3 is gone
	_, _, err = reg.Pull(ctx, "app:v3")
	if err == nil {
		t.Fatal("expected unreferenced image v3 to be deleted, but still found")
	}
}

func TestGCPolicy_BackgroundGCRunsDecoupledWithoutDisruptingDeployments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := zerolog.Nop()
	reg := registry.NewMemoryRegistry(log)

	provider := registry.NewMemoryDigestProvider()
	policy := registry.NewGCPolicy(provider, reg, log)

	// Start background GC loop running every 10ms
	policy.StartBackgroundLoop(ctx, 10*time.Millisecond)

	// Simulate concurrent active deployments pushing images and registering digests
	var wg sync.WaitGroup
	var pushErrors int
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("deployment-payload-%d", idx))
			digest := registry.ComputeDigest(data)
			tag := fmt.Sprintf("tenant/app:%d", idx)

			// Register as active deployment
			provider.AddDigest(digest)

			// Push to registry
			_, err := reg.Push(ctx, tag, data, digest)
			if err != nil {
				mu.Lock()
				pushErrors++
				mu.Unlock()
			}

			// Verify immediately pullable
			_, _, err = reg.Pull(ctx, tag)
			if err != nil {
				mu.Lock()
				pushErrors++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	if pushErrors > 0 {
		t.Fatalf("background GC disrupted concurrent deployments: %d errors", pushErrors)
	}
}
