package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

type mockTestRegistry struct {
	available bool
	images    map[string]string // tag -> digest
	calls     int
}

func (r *mockTestRegistry) HasImage(ctx context.Context, tag string) bool {
	r.calls++
	if !r.available {
		return false
	}
	_, ok := r.images[tag]
	return ok
}

func (r *mockTestRegistry) GetDigest(ctx context.Context, tag string) (string, error) {
	r.calls++
	if !r.available {
		return "", errors.New("registry service unavailable (503)")
	}
	d, ok := r.images[tag]
	if !ok {
		return "", errors.New("image not found")
	}
	return d, nil
}

// TestImageCache_Hit verifies that when an image is cached locally,
// the container runs immediately without hitting the remote registry.
func TestImageCache_Hit(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	reg := &mockTestRegistry{
		available: true,
		images: map[string]string{
			"app:v1": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
	}

	client := NewMockDockerClient()
	client.ImageCache = map[string]bool{
		"app:v1": true, // Pre-cached image
	}
	client.Registry = reg

	tracker := NewInstanceTracker()
	ops := NewContainerOps(client, tracker, log)

	res, err := ops.RunContainer(ctx, RunOptions{
		InstanceID:   "inst-cache-hit",
		DeploymentID: "dep-01",
		Image:        "app:v1",
	})
	if err != nil {
		t.Fatalf("RunContainer failed on cache hit: %v", err)
	}

	if res.Status != string(StateRunning) {
		t.Fatalf("expected StateRunning, got %s", res.Status)
	}

	if client.PullCalls != 0 {
		t.Fatalf("expected 0 pull calls on cache hit, got %d", client.PullCalls)
	}
	if reg.calls != 0 {
		t.Fatalf("expected 0 registry calls on cache hit, got %d", reg.calls)
	}
}

// TestImageCache_Miss_SuccessfulPull verifies that when an image is missing locally,
// the worker agent queries the remote registry, pulls by digest, populates the local cache,
// and successfully runs the container.
func TestImageCache_Miss_SuccessfulPull(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	digest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	reg := &mockTestRegistry{
		available: true,
		images: map[string]string{
			"app:v2": digest,
		},
	}

	client := NewMockDockerClient()
	client.ImageCache = make(map[string]bool) // Empty local cache
	client.Registry = reg

	tracker := NewInstanceTracker()
	ops := NewContainerOps(client, tracker, log)

	// Run container with digest label
	res, err := ops.RunContainer(ctx, RunOptions{
		InstanceID:   "inst-cache-miss",
		DeploymentID: "dep-02",
		Image:        "app:v2",
		Labels: map[string]string{
			"nebula.image_digest": digest,
		},
	})
	if err != nil {
		t.Fatalf("RunContainer failed on cache miss pull: %v", err)
	}

	if res.Status != string(StateRunning) {
		t.Fatalf("expected StateRunning, got %s", res.Status)
	}

	if client.PullCalls != 1 {
		t.Fatalf("expected 1 pull call on cache miss, got %d", client.PullCalls)
	}
	if !client.ImageCache["app:v2"] {
		t.Fatalf("expected image to be populated in local cache after pull")
	}

	// Subsequent invocation for another instance with the same image should now HIT cache
	res2, err := ops.RunContainer(ctx, RunOptions{
		InstanceID:   "inst-cache-second",
		DeploymentID: "dep-02",
		Image:        "app:v2",
	})
	if err != nil {
		t.Fatalf("RunContainer second run failed: %v", err)
	}
	if res2.Status != string(StateRunning) {
		t.Fatalf("expected StateRunning, got %s", res2.Status)
	}
	if client.PullCalls != 1 {
		t.Fatalf("expected pull calls to remain 1 after cache populated, got %d", client.PullCalls)
	}
}

// TestImageCache_Miss_RegistryUnavailable verifies that a cache miss against an unavailable
// registry fails immediately without false success and without creating the container.
func TestImageCache_Miss_RegistryUnavailable(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	reg := &mockTestRegistry{
		available: false, // Registry offline
		images:    map[string]string{"app:v3": "sha256:3333"},
	}

	client := NewMockDockerClient()
	client.ImageCache = make(map[string]bool)
	client.Registry = reg

	tracker := NewInstanceTracker()
	ops := NewContainerOps(client, tracker, log)

	res, err := ops.RunContainer(ctx, RunOptions{
		InstanceID:   "inst-cache-miss-offline",
		DeploymentID: "dep-03",
		Image:        "app:v3",
	})
	if err == nil {
		t.Fatalf("expected failure when registry unavailable on cache miss, got nil")
	}

	if client.CreateCalls != 0 {
		t.Fatalf("expected 0 CreateContainer calls when registry unavailable, got %d", client.CreateCalls)
	}

	if res.Status != string(StateFailed) {
		t.Fatalf("expected status FAILED, got %s", res.Status)
	}

	if !strings.Contains(res.Error, "registry pull failed") && !strings.Contains(res.Error, "unavailable") {
		t.Fatalf("expected error to mention registry pull failure, got: %s", res.Error)
	}
}
