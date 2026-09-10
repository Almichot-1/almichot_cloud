package registry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestRemoteRegistryClient_Fallback(t *testing.T) {
	log := zerolog.Nop()
	cfg := RemoteRegistryConfig{
		RegistryHost: "", // in-memory fallback
	}
	client := NewRemoteRegistryClient(cfg, nil, log)
	ctx := context.Background()

	payload := []byte("image-data-payload-123")
	digest, err := client.Push(ctx, "app:v1", payload, "")
	if err != nil {
		t.Fatalf("push failed: %v", err)
	}

	if !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("unexpected digest format: %s", digest)
	}

	gotDigest, err := client.GetDigest(ctx, "app:v1")
	if err != nil || gotDigest != digest {
		t.Fatalf("GetDigest: want %s, got %s (err: %v)", digest, gotDigest, err)
	}

	if !client.HasImage(ctx, "app:v1") {
		t.Fatalf("expected HasImage to return true")
	}

	pulledData, pulledDigest, err := client.Pull(ctx, "app:v1")
	if err != nil || string(pulledData) != string(payload) || pulledDigest != digest {
		t.Fatalf("Pull mismatch: %s / %s (err: %v)", string(pulledData), pulledDigest, err)
	}
}

func TestRemoteRegistryClient_OCI_HTTP(t *testing.T) {
	log := zerolog.Nop()
	expectedDigest := "sha256:abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"
	manifestContent := `{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}`

	// Create mock OCI Distribution v2 Registry HTTP Server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v2/myproject/manifests/v1") {
			w.Header().Set("Docker-Content-Digest", expectedDigest)
			w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(manifestContent))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	cfg := RemoteRegistryConfig{
		RegistryHost: host,
		Insecure:     true,
	}

	client := NewRemoteRegistryClient(cfg, nil, log)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	digest, err := client.GetDigest(ctx, "myproject:v1")
	if err != nil {
		t.Fatalf("failed to query remote registry manifest: %v", err)
	}
	if digest != expectedDigest {
		t.Fatalf("GetDigest: expected %s, got %s", expectedDigest, digest)
	}

	data, pullDigest, err := client.Pull(ctx, "myproject:v1")
	if err != nil {
		t.Fatalf("failed to pull remote manifest: %v", err)
	}
	if string(data) != manifestContent || pullDigest != expectedDigest {
		t.Fatalf("Pull manifest mismatch: got data %s, digest %s", string(data), pullDigest)
	}
}

func TestRemoteRegistryClient_AuthFailure(t *testing.T) {
	log := zerolog.Nop()

	// Mock server returning HTTP 401 Unauthorized for bad credentials
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "valid-user" || pass != "valid-secret" {
			w.Header().Set("WWW-Authenticate", `Basic realm="Registry Realm"`)
			http.Error(w, `{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`, http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	cfg := RemoteRegistryConfig{
		RegistryHost: host,
		Username:     "wrong-user",
		Password:     "wrong-pass",
		Insecure:     true,
	}

	client := NewRemoteRegistryClient(cfg, nil, log)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Push should fail with actionable authentication error, not hang (§10.2, §15)
	_, err := client.Push(ctx, "myproject:v1", []byte("data"), "")
	if err == nil {
		t.Fatalf("expected push to fail on 401 Unauthorized")
	}
	if !strings.Contains(err.Error(), "401") && !strings.Contains(strings.ToLower(err.Error()), "unauthorized") && !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("expected actionable authentication error, got: %v", err)
	}
}

func TestGarbageCollection_PurgeUnreferenced(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()
	reg := NewMemoryRegistry(log)

	// Push 3 images
	_, _ = reg.Push(ctx, "app:v1", []byte("img-1"), "")
	d2, _ := reg.Push(ctx, "app:v2", []byte("img-2"), "")
	_, _ = reg.Push(ctx, "app:v3", []byte("img-3"), "")

	// Only d2 is referenced by an active deployment
	res, err := reg.GarbageCollect(ctx, []string{d2})
	if err != nil {
		t.Fatalf("GarbageCollect failed: %v", err)
	}

	if res.DeletedImages != 2 {
		t.Fatalf("expected 2 deleted images, got %d", res.DeletedImages)
	}
	if res.RetainedImages != 1 {
		t.Fatalf("expected 1 retained image, got %d", res.RetainedImages)
	}

	// Active deployment image must still exist
	if !reg.HasImage(ctx, "app:v2") {
		t.Fatalf("expected active image app:v2 to be retained")
	}
	gotD2, err := reg.GetDigest(ctx, "app:v2")
	if err != nil || gotD2 != d2 {
		t.Fatalf("expected app:v2 digest to match d2: %v", err)
	}

	// Unreferenced images must be purged
	if reg.HasImage(ctx, "app:v1") || reg.HasImage(ctx, "app:v3") {
		t.Fatalf("expected unreferenced images to be removed")
	}
	if _, err := reg.GetDigest(ctx, "app:v1"); !errors.Is(err, ErrImageNotFound) {
		t.Fatalf("expected ErrImageNotFound for purged image, got: %v", err)
	}
}

