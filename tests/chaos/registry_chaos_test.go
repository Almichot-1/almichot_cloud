package chaos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/rs/zerolog"
)

// TestRegistryChaos_MidPullSeveranceOnUncachedWorker verifies that if the registry
// connection is severed mid-pull on a fresh worker with no local cache, the failure
// surfaces immediately as an error event rather than hanging indefinitely (§11, §23.3).
func TestRegistryChaos_MidPullSeveranceOnUncachedWorker(t *testing.T) {
	log := zerolog.Nop()

	// Server that closes connection abruptly during pull
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, _ := hj.Hijack()
			_ = conn.Close() // Sever TCP connection mid-stream
			return
		}
		http.Error(w, "server down", http.StatusInternalServerError)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	regClient := registry.NewRemoteRegistryClient(registry.RemoteRegistryConfig{
		RegistryHost: host,
		Insecure:     true,
	}, nil, log)

	workerClient := runtime.NewMockDockerClient()
	workerClient.ImageCache = make(map[string]bool) // Empty cache
	workerClient.Registry = regClient

	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(workerClient, tracker, log)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	res, err := ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   "inst-chaos-midpull",
		DeploymentID: "dep-chaos",
		Image:        "app:v1",
	})
	elapsed := time.Since(start)

	// Ensure no hang
	if elapsed > 4*time.Second {
		t.Fatalf("worker hung on severed registry connection for %v", elapsed)
	}

	if err == nil {
		t.Fatalf("expected error on severed registry connection mid-pull, got nil")
	}

	if res.Status != string(runtime.StateFailed) {
		t.Fatalf("expected status FAILED, got %s", res.Status)
	}

	if workerClient.CreateCalls != 0 {
		t.Fatalf("container must not be created when registry severed mid-pull")
	}
}

// TestRegistryChaos_CachedImageSurvivesRegistryOutage verifies that when a worker
// already has the image in its local cache, it successfully creates and starts the
// container even when the remote registry is completely dead (§11, §23.3).
func TestRegistryChaos_CachedImageSurvivesRegistryOutage(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// 1. Registry is completely down / unreachable
	deadHost := "127.0.0.1:59999" // Closed port
	deadRegClient := registry.NewRemoteRegistryClient(registry.RemoteRegistryConfig{
		RegistryHost: deadHost,
		Insecure:     true,
	}, nil, log)

	imageName := "nebula/critical-app:v2.0"
	imageDigest := "sha256:abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"

	// 2. Worker Agent has the image already pre-cached locally
	workerClient := runtime.NewMockDockerClient()
	workerClient.ImageCache = map[string]bool{
		imageName:   true,
		imageDigest: true,
	}
	workerClient.Registry = deadRegClient

	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(workerClient, tracker, log)

	// 3. Worker executes RunContainer during the total registry outage
	res, err := ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   "inst-chaos-cached-resilient",
		DeploymentID: "dep-cached-resilient",
		Image:        imageName,
		Labels: map[string]string{
			"nebula.image_digest": imageDigest,
		},
	})

	if err != nil {
		t.Fatalf("cached image failed to start during registry outage: %v", err)
	}

	if res.Status != string(runtime.StateRunning) {
		t.Fatalf("expected status RUNNING, got %s", res.Status)
	}

	if workerClient.PullCalls != 0 {
		t.Fatalf("expected 0 pull calls on cache hit, got %d", workerClient.PullCalls)
	}

	if workerClient.CreateCalls != 1 {
		t.Fatalf("expected container to be created from local cache, got %d", workerClient.CreateCalls)
	}
}
