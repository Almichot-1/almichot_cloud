package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

type testStack struct {
	srv      *httptest.Server
	registry *workers.Registry
	depRepo  deployments.DeploymentRepository
	instRepo deployments.InstanceRepository
}

func newTestStack(t *testing.T) *testStack {
	t.Helper()
	log := zerolog.Nop()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	projectRepo := projects.NewMemoryProjectRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()
	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockFactory, log)

	router := loadbalancer.NewRouter(log)
	orchestrator := build.NewOrchestrator(build.NewEphemeralSandbox(log), log)
	depService.SetBuildAndRegistry(orchestrator, depService.RegistryClient(), router)

	srv := httptest.NewServer(NewRouter(NewServer(depService, projectRepo, reg, router, log)))
	t.Cleanup(srv.Close)

	return &testStack{srv: srv, registry: reg, depRepo: depRepo, instRepo: instRepo}
}

// HTTP API happy path: project creation, deployment pipeline, and read APIs
// with in-memory repositories and a mocked worker dispatch (no Docker/Postgres).
func TestHTTPServer_CreateProjectDeployAndRead(t *testing.T) {
	stack := newTestStack(t)
	ctx := context.Background()

	if _, err := stack.registry.Register(ctx, workers.RegisterParams{
		WorkerKey: "api-worker-1",
		Hostname:  "api-node-1",
		IPAddress: "127.0.0.1",
		Capacity:  10,
	}); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	// 1. Create project.
	projectID := mustCreateProject(t, stack.srv, "api-app")

	// Create again with the same name must conflict.
	if resp, _ := http.Post(stack.srv.URL+"/v1/projects", "application/json", ctxReader(`{"name":"api-app"}`)); resp.StatusCode != http.StatusConflict {
		t.Errorf("duplicate project: expected 409, got %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// 2. Deploy a Go source repo (simulated build, mocked dispatch).
	source := t.TempDir()
	_ = os.WriteFile(filepath.Join(source, "go.mod"), []byte("module api\n\ngo 1.22\n"), 0644)
	_ = os.WriteFile(filepath.Join(source, "main.go"), []byte("package main\nfunc main(){}"), 0644)

	payload := fmt.Sprintf(`{"source_path": %q, "instance_count": 1}`, source)
	resp, err := http.Post(stack.srv.URL+"/v1/projects/"+projectID+"/deployments", "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("deploy: status %d body %s", resp.StatusCode, string(body))
	}

	var dep struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		ImageDigest string `json:"image_digest"`
		Instances   []struct {
			Status string `json:"status"`
		} `json:"instances"`
	}
	if err := json.Unmarshal(body, &dep); err != nil {
		t.Fatalf("decode deploy response: %v", err)
	}
	if dep.Status != "RUNNING" {
		t.Errorf("deploy status: got %q, want RUNNING", dep.Status)
	}
	if !strings.HasPrefix(dep.ImageDigest, "sha256:") {
		t.Errorf("image digest missing: %q", dep.ImageDigest)
	}
	if len(dep.Instances) != 1 || dep.Instances[0].Status != "RUNNING" {
		t.Errorf("instances: %+v", dep.Instances)
	}

	// 3. GET deployment.
	gresp, err := http.Get(stack.srv.URL + "/v1/deployments/" + dep.ID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	defer gresp.Body.Close()
	var gotDep struct {
		Status      string `json:"status"`
		ImageDigest string `json:"image_digest"`
	}
	if err := json.NewDecoder(gresp.Body).Decode(&gotDep); err != nil {
		t.Fatalf("decode get deployment: %v", err)
	}
	if gotDep.Status != "RUNNING" {
		t.Errorf("GET deployment status: got %q, want RUNNING", gotDep.Status)
	}
	if gotDep.ImageDigest != dep.ImageDigest {
		t.Errorf("GET deployment digest: got %q, want %q", gotDep.ImageDigest, dep.ImageDigest)
	}

	// 4. Repos hold the record.
	stored, err := stack.depRepo.GetByID(ctx, dep.ID)
	if err != nil {
		t.Fatalf("deployment repo: %v", err)
	}
	if stored.ImageDigest != dep.ImageDigest {
		t.Errorf("repo digest: got %q", stored.ImageDigest)
	}

	// 5. Workers and projects listings.
	wresp, err := http.Get(stack.srv.URL + "/v1/workers")
	if err != nil {
		t.Fatalf("list workers: %v", err)
	}
	defer wresp.Body.Close()
	wdata, _ := io.ReadAll(wresp.Body)
	if !strings.Contains(string(wdata), "api-worker-1") {
		t.Errorf("workers list missing worker: %s", string(wdata))
	}

	presp, err := http.Get(stack.srv.URL + "/v1/projects")
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	defer presp.Body.Close()
	pdata, _ := io.ReadAll(presp.Body)
	if !strings.Contains(string(pdata), "api-app") {
		t.Errorf("projects list missing project: %s", string(pdata))
	}
}

func TestHTTPServer_DeployUnknownProject404(t *testing.T) {
	stack := newTestStack(t)
	resp, err := http.Post(stack.srv.URL+"/v1/projects/does-not-exist/deployments", "application/json",
		ctxReader(`{"image":"nginx:alpine","instance_count":1}`))
	if err != nil {
		t.Fatalf("deploy unknown project: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for unknown project, got %d", resp.StatusCode)
	}
}

func mustCreateProject(t *testing.T, srv *httptest.Server, name string) string {
	t.Helper()
	payload := fmt.Sprintf(`{"name": %q, "description": "api test"}`, name)
	resp, err := http.Post(srv.URL+"/v1/projects", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /v1/projects: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create project: status %d body %s", resp.StatusCode, string(body))
	}
	var p struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		t.Fatalf("decode project: %v", err)
	}
	if p.ID == "" {
		t.Fatal("create project returned empty id")
	}
	return p.ID
}

func ctxReader(s string) io.Reader {
	return bytes.NewReader([]byte(s))
}