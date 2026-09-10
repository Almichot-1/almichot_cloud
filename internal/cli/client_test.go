package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/cli"
)

func TestCLI_ConfigSaveAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	orig := &cli.Config{
		Endpoint:       "http://127.0.0.1:8080",
		Token:          "secret-token-123",
		DefaultProject: "proj-xyz",
	}

	if err := cli.SaveConfig(configPath, orig); err != nil {
		t.Fatalf("failed to save config: %v", err)
	}

	loaded, err := cli.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if loaded.Endpoint != orig.Endpoint || loaded.Token != orig.Token || loaded.DefaultProject != orig.DefaultProject {
		t.Fatalf("loaded config mismatch: %+v vs %+v", loaded, orig)
	}
}

func TestCLI_Login_SuccessAndFailure(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer valid-token" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized: invalid token"}`))
		}
	}))
	defer server.Close()

	client := cli.NewClient(server.URL, "")

	// 1. Success
	if err := client.Login(ctx, "valid-token"); err != nil {
		t.Fatalf("expected login success, got error: %v", err)
	}

	// 2. Failure: clear error message, no stack trace
	err := client.Login(ctx, "bad-token")
	if err == nil {
		t.Fatal("expected login failure for bad token, got nil")
	}
	if !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("expected clear user-facing error message, got: %v", err)
	}
}

func TestCLI_InitProject_Contract(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/projects" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var payload map[string]string
		_ = json.NewDecoder(r.Body).Decode(&payload)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(cli.Project{
			ID:            "proj-created-123",
			Name:          payload["name"],
			DefaultBranch: payload["default_branch"],
			RootDir:       payload["root_dir"],
			CreatedAt:     time.Now(),
		})
	}))
	defer server.Close()

	client := cli.NewClient(server.URL, "test-token")
	proj, err := client.InitProject(ctx, "my-app", "main", ".")
	if err != nil {
		t.Fatalf("failed to init project: %v", err)
	}

	if proj.ID != "proj-created-123" || proj.Name != "my-app" {
		t.Fatalf("unexpected project data: %+v", proj)
	}
}

func TestCLI_DeployAndStatus_Contract(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/deployments"):
			_ = json.NewEncoder(w).Encode(cli.Deployment{
				ID:            "dep-999",
				ProjectID:     "proj-1",
				Image:         "registry.nebula/app:v1",
				Status:        "RUNNING",
				Stage:         "RUNNING",
				InstanceCount: 2,
				RunningCount:  2,
				CreatedAt:     time.Now(),
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects/proj-1":
			_ = json.NewEncoder(w).Encode(cli.Project{
				ID:   "proj-1",
				Name: "test-proj",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects/proj-1/deployments":
			_ = json.NewEncoder(w).Encode([]cli.Deployment{
				{
					ID:            "dep-999",
					ProjectID:     "proj-1",
					Image:         "registry.nebula/app:v1",
					Status:        "RUNNING",
					Stage:         "RUNNING",
					InstanceCount: 2,
					RunningCount:  2,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := cli.NewClient(server.URL, "token")

	// Deploy
	dep, err := client.Deploy(ctx, cli.DeployParams{
		ProjectID:     "proj-1",
		Image:         "registry.nebula/app:v1",
		InstanceCount: 2,
	})
	if err != nil {
		t.Fatalf("Deploy failed: %v", err)
	}
	if dep.ID != "dep-999" || dep.Status != "RUNNING" {
		t.Fatalf("unexpected deployment state: %+v", dep)
	}

	// Status
	status, err := client.Status(ctx, "proj-1")
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if status.Project.Name != "test-proj" || status.LatestDeployment == nil || status.LatestDeployment.Status != "RUNNING" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestCLI_Logs_StreamingContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/logs") {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("expected flusher")
		}

		lines := []string{
			"[2026-09-10T12:00:00Z] [INFO] BUILD_STARTED: Building container image",
			"[2026-09-10T12:00:01Z] [INFO] SCHEDULING: Selecting worker node",
			"[2026-09-10T12:00:02Z] [INFO] RUNNING: Container instance is healthy",
		}

		for _, l := range lines {
			_, _ = fmt.Fprintln(w, l)
			flusher.Flush()
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer server.Close()

	client := cli.NewClient(server.URL, "token")
	var out bytes.Buffer

	err := client.Logs(ctx, "proj-1", "", true, &out)
	if err != nil && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("Logs streaming failed: %v", err)
	}

	logText := out.String()
	if !strings.Contains(logText, "BUILD_STARTED") || !strings.Contains(logText, "Container instance is healthy") {
		t.Fatalf("expected streamed log lines, got: %s", logText)
	}
}

func TestCLI_VersionMismatch_ErrorFormatting(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientVer := r.Header.Get(cli.HeaderClientVersion)
		if clientVer == "v0.1.0" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUpgradeRequired)
			_, _ = w.Write([]byte(`{"error":"client version mismatch: v0.1.0 is deprecated, please upgrade nebula-cli to >= v1.0.0"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := cli.NewClient(server.URL, "token")
	client.ClientVersion = "v0.1.0" // Deprecated version

	var dummy cli.Project
	err := client.Login(ctx, "token")
	if err == nil {
		t.Fatal("expected version mismatch error for v0.1.0, got nil")
	}

	if !strings.Contains(err.Error(), "client version mismatch") || !strings.Contains(err.Error(), "please upgrade") {
		t.Fatalf("expected actionable version mismatch message, got: %v", err)
	}
	_ = dummy
}
