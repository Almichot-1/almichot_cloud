// Package integration contains end-to-end tests that exercise the control plane
// against real infrastructure: PostgreSQL persistence, a real Docker daemon build,
// real container execution, and real HTTP traffic through the load balancer.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/api"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/storage"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// TestRealEndToEnd_HTTPBuildPostgresRunAndReachability drives the full pipeline with
// zero mocks in the hot path:
//
//	HTTP API → Project (Postgres) → Real Docker image build → Registry digest verification
//	→ Scheduler → Real container run (published port) → Postgres Deployment/Instance
//	→ Load balancer → real HTTP 200 from the running container.
//
// Requires NEBULA_TEST_DATABASE_URL (Postgres) and a reachable Docker daemon; skips otherwise.
func TestRealEndToEnd_HTTPBuildPostgresRunAndReachability(t *testing.T) {
	adminURL := os.Getenv("NEBULA_TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("NEBULA_TEST_DATABASE_URL not set; skipping real E2E")
	}

	log := zerolog.Nop()

	// --- Postgres throwaway database ------------------------------------------------
	adminU, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("parse admin URL: %v", err)
	}
	adminConn, err := pgx.Connect(context.Background(), adminURL)
	if err != nil {
		t.Fatalf("connect to admin db: %v", err)
	}
	dbName := fmt.Sprintf("nebula_e2e_%d", time.Now().UnixNano())
	ctxConn, cancelConn := context.WithTimeout(context.Background(), 30*time.Second)
	if _, err := adminConn.Exec(ctxConn, "CREATE DATABASE "+dbName); err != nil {
		cancelConn()
		t.Fatalf("create throwaway db: %v", err)
	}
	cancelConn()
	t.Cleanup(func() {
		ctxDrop, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_, _ = adminConn.Exec(ctxDrop, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
		_ = adminConn.Close(context.Background())
	})

	target := *adminU
	target.Path = "/" + dbName
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, target.String())
	if err != nil {
		t.Fatalf("pool for throwaway db: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := storage.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	projectRepo := storage.NewPostgresProjectRepository(pool)
	workerRepo := storage.NewPostgresWorkerRepository(pool)
	depRepo := storage.NewPostgresDeploymentRepository(pool)
	instRepo := storage.NewPostgresInstanceRepository(pool)

	// --- Real Docker worker ----------------------------------------------------------
	dockerClient, err := runtime.NewRealDockerClient(log)
	if err != nil {
		t.Skipf("skipping real E2E: docker daemon unavailable: %v", err)
	}
	t.Cleanup(func() { _ = dockerClient.Close() })

	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(dockerClient, tracker, log)
	clientFactory := &realWorkerClientFactory{ops: ops}

	// --- Control plane components -----------------------------------------------------
	registry := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(registry, instRepo.CountByWorkerForDeployment, log)
	depService := deployments.NewService(depRepo, instRepo, registry, sched, clientFactory, log)

	realSandbox, err := build.NewRealDockerSandbox(log)
	if err != nil {
		t.Skipf("skipping real E2E: docker daemon unreachable: %v", err)
	}
	orchestrator := build.NewOrchestrator(realSandbox, log)
	router := loadbalancer.NewRouter(log)
	depService.SetBuildAndRegistry(orchestrator, depService.RegistryClient(), router)
	t.Cleanup(func() { _ = realSandbox.Close() })

	// --- HTTP API ---------------------------------------------------------------------
	apiServer := api.NewServer(depService, projectRepo, registry, router, log)
	httpSrv := httptest.NewServer(api.NewRouter(apiServer))
	defer httpSrv.Close()

	// 1. Create project via HTTP → persisted to Postgres.
	projectID := createProjectViaHTTP(t, httpSrv.URL, "real-e2e-app")

	// 2. Register the worker through the registry (persisted via PostgresWorkerRepository).
	if _, err := registry.Register(ctx, workers.RegisterParams{
		WorkerKey: "e2e-docker-worker",
		Hostname:  "e2e-node-1",
		IPAddress: "localhost",
		Capacity:  10,
	}); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	// 3. Real source repo: nginx serving a marker page for any path on port 80 (Dockerfile strategy).
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.html"), []byte("nebula-real-http-ok"), 0644); err != nil {
		t.Fatalf("write index.html: %v", err)
	}
	nginxConf := `server {
  listen 80;
  server_name _;
  root /usr/share/nginx/html;
  location / {
    try_files $uri /index.html;
  }
}
`
	if err := os.WriteFile(filepath.Join(source, "nginx.conf"), []byte(nginxConf), 0644); err != nil {
		t.Fatalf("write nginx.conf: %v", err)
	}
	dockerfile := `FROM nginx:alpine
COPY index.html /usr/share/nginx/html/index.html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
`
	if err := os.WriteFile(filepath.Join(source, "Dockerfile"), []byte(dockerfile), 0644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	// 4. Deploy via HTTP API → real build → registry → schedule → real container run.
	deployResp := deployViaHTTP(t, httpSrv.URL, projectID, source)
	if deployResp.Status != "RUNNING" {
		t.Fatalf("expected deployment RUNNING, got %q (stage=%q)", deployResp.Status, deployResp.Stage)
	}
	if deployResp.ImageDigest == "" || !strings.HasPrefix(deployResp.ImageDigest, "sha256:") {
		t.Fatalf("expected real image digest, got %q", deployResp.ImageDigest)
	}
	if len(deployResp.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(deployResp.Instances))
	}
	inst := deployResp.Instances[0]
	if inst.Status != "RUNNING" {
		t.Fatalf("instance status %q, want RUNNING", inst.Status)
	}

	// 5. The real container must answer HTTP on its published port.
	targetURLs := router.GetTargets(projectID)
	if len(targetURLs) != 1 {
		t.Fatalf("expected 1 LB target, got %v", targetURLs)
	}
	if !waitReachable(t, targetURLs[0], 90*time.Second) {
		t.Fatalf("real container never became reachable at %s", targetURLs[0])
	}

	// 6. Traffic through the load balancer reaches the real container.
	// Path-based routing: /services/{project}/... (nginx falls back to index.html for any path).
	lbReq, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/services/"+projectID+"/index.html", nil)
	lbResp, err := http.DefaultClient.Do(lbReq)
	if err != nil {
		t.Fatalf("load balancer request failed: %v", err)
	}
	defer lbResp.Body.Close()
	body, _ := io.ReadAll(lbResp.Body)
	if lbResp.StatusCode != http.StatusOK {
		t.Fatalf("LB expected 200, got %d (body=%s)", lbResp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), "nebula-real-http-ok") {
		t.Fatalf("LB response did not come from the real container: %s", string(body))
	}
	if got := lbResp.Header.Get("X-Forwarded-By"); got != "nebula-loadbalancer" {
		t.Fatalf("expected X-Forwarded-By header, got %q", got)
	}

	// 7. Postgres persisted the verified digest.
	persistedDep, err := depRepo.GetByID(ctx, deployResp.ID)
	if err != nil {
		t.Fatalf("fetch deployment from postgres: %v", err)
	}
	if persistedDep.ImageDigest != deployResp.ImageDigest {
		t.Fatalf("postgres digest mismatch: stored %q vs api %q", persistedDep.ImageDigest, deployResp.ImageDigest)
	}
	if persistedDep.Status != deployments.StatusRunning {
		t.Fatalf("postgres deployment status %q, want RUNNING", persistedDep.Status)
	}
	persistedInsts, err := instRepo.ListByDeployment(ctx, deployResp.ID)
	if err != nil || len(persistedInsts) != 1 {
		t.Fatalf("postgres instances: n=%d err=%v", len(persistedInsts), err)
	}
	if persistedInsts[0].Status != "RUNNING" {
		t.Fatalf("postgres instance status %q, want RUNNING", persistedInsts[0].Status)
	}

	// 8. The registry holds the image with the same verified digest.
	storedDigest, err := depService.RegistryClient().GetDigest(ctx, persistedDep.Image)
	if err != nil || storedDigest != deployResp.ImageDigest {
		t.Fatalf("registry digest %q (%v) does not match api digest %q", storedDigest, err, deployResp.ImageDigest)
	}

	// 9. API surfaces the deployment and project from Postgres.
	if got := getDeploymentStatusViaHTTP(t, httpSrv.URL, deployResp.ID); got != "RUNNING" {
		t.Fatalf("GET /v1/deployments status %q, want RUNNING", got)
	}
	if got := listProjectsViaHTTP(t, httpSrv.URL); !strings.Contains(got, projectID) {
		t.Fatalf("projects list did not include %s: %s", projectID, got)
	}

	// Cleanup the running container.
	for _, targetURL := range targetURLs {
		_ = stopAndRemoveContainer(t, ops, targetURL)
	}

	t.Logf("REAL E2E PASSED: HTTP → project(PG) → docker build(%s) → registry → run(target=%s) → LB 200, digest=%s",
		persistedDep.Image, targetURLs[0], persistedDep.ImageDigest)
}

// ----------------------------------------------------------------------------------
// Test doubles
// ----------------------------------------------------------------------------------

type realWorkerClientFactory struct {
	ops *runtime.ContainerOps
}

func (f *realWorkerClientFactory) GetClient(ctx context.Context, worker *workers.Worker) (deployments.WorkerClient, error) {
	return &realWorkerClient{ops: f.ops}, nil
}

type realWorkerClient struct {
	ops *runtime.ContainerOps
}

func (c *realWorkerClient) RunContainer(ctx context.Context, req *proto.RunContainerRequest, opts ...grpc.CallOption) (*proto.RunContainerResponse, error) {
	env := make([]string, 0, len(req.Env))
	for k, v := range req.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	ports := make([]runtime.PortMapping, 0, len(req.Ports))
	for _, p := range req.Ports {
		ports = append(ports, runtime.PortMapping{HostPort: int(p.HostPort), ContainerPort: int(p.ContainerPort), Protocol: p.Protocol})
	}

	res, err := c.ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   req.InstanceId,
		DeploymentID: req.DeploymentId,
		Image:        req.Image,
		Env:          env,
		Ports:        ports,
		Labels:       req.Labels,
	})
	if err != nil {
		return &proto.RunContainerResponse{InstanceId: req.InstanceId, Status: "FAILED", Error: err.Error()}, nil
	}
	return &proto.RunContainerResponse{
		InstanceId:  res.InstanceID,
		ContainerId: res.ContainerID,
		Status:      res.Status,
		IsDuplicate: res.IsDuplicate,
	}, nil
}

func (c *realWorkerClient) StopContainer(ctx context.Context, req *proto.StopContainerRequest, opts ...grpc.CallOption) (*proto.StopContainerResponse, error) {
	res, err := c.ops.StopContainer(ctx, req.InstanceId, time.Duration(req.TimeoutSeconds)*time.Second)
	if err != nil {
		return &proto.StopContainerResponse{InstanceId: req.InstanceId, Success: false, Error: err.Error()}, nil
	}
	return &proto.StopContainerResponse{InstanceId: res.InstanceID, Success: res.Success}, nil
}

func (c *realWorkerClient) ListContainers(ctx context.Context, req *proto.ListContainersRequest, opts ...grpc.CallOption) (*proto.ListContainersResponse, error) {
	if client := c.ops.Client(); client != nil {
		summaries, err := client.ListContainers(ctx)
		if err == nil {
			var containers []*proto.ContainerInfo
			for _, sum := range summaries {
				instanceID := sum.InstanceID
				if instanceID == "" && sum.Labels != nil {
					instanceID = sum.Labels["nebula.instance_id"]
				}
				containers = append(containers, &proto.ContainerInfo{
					InstanceId:  instanceID,
					ContainerId: sum.ID,
					Image:       sum.Image,
					Status:      sum.State,
					Labels:      sum.Labels,
					CreatedAt:   sum.CreatedAt.Unix(),
				})
			}
			return &proto.ListContainersResponse{Containers: containers}, nil
		}
	}
	recs, err := c.ops.ListContainers(ctx)
	if err != nil {
		return nil, err
	}
	var containers []*proto.ContainerInfo
	for _, rec := range recs {
		containers = append(containers, &proto.ContainerInfo{
			InstanceId:  rec.InstanceID,
			ContainerId: rec.ContainerID,
			Image:       rec.Image,
			Status:      string(rec.State),
			Labels:      rec.Labels,
			CreatedAt:   rec.CreatedAt.Unix(),
		})
	}
	return &proto.ListContainersResponse{Containers: containers}, nil
}

// ----------------------------------------------------------------------------------

// HTTP helpers
// ----------------------------------------------------------------------------------

type projectCreateBody struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	RepoURL     string `json:"repo_url"`
}

type projectAPIResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func createProjectViaHTTP(t *testing.T, baseURL, name string) string {
	t.Helper()
	body, _ := json.Marshal(projectCreateBody{Name: name, Description: "real e2e", RepoURL: "https://example.com/repo"})
	resp, err := http.Post(baseURL+"/v1/projects", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("create project status %d: %s", resp.StatusCode, string(data))
	}
	var p projectAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		t.Fatalf("decode project: %v", err)
	}
	if p.ID == "" {
		t.Fatal("create project returned empty id")
	}
	return p.ID
}

type deploymentAPIResponse struct {
	ID          string `json:"id"`
	ProjectID   string `json:"project_id"`
	Image       string `json:"image"`
	ImageDigest string `json:"image_digest"`
	Status      string `json:"status"`
	Stage       string `json:"stage"`
	Instances   []struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		ContainerID string `json:"container_id"`
	} `json:"instances"`
}

func deployViaHTTP(t *testing.T, baseURL, projectID, source string) deploymentAPIResponse {
	t.Helper()
	payload := fmt.Sprintf(`{"source_path": %q, "instance_count": 1}`, source)
	resp, err := http.Post(baseURL+"/v1/projects/"+projectID+"/deployments", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("deploy status %d: %s", resp.StatusCode, string(data))
	}
	var dep deploymentAPIResponse
	if err := json.Unmarshal(data, &dep); err != nil {
		t.Fatalf("decode deployment: %v", err)
	}
	if dep.ID == "" {
		t.Fatal("deployment returned empty id")
	}
	return dep
}

func getDeploymentStatusViaHTTP(t *testing.T, baseURL, deploymentID string) string {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/deployments/" + deploymentID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	defer resp.Body.Close()
	var dep struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&dep); err != nil {
		t.Fatalf("decode deployment status: %v", err)
	}
	return dep.Status
}

func listProjectsViaHTTP(t *testing.T, baseURL string) string {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/projects")
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return string(data)
}

func waitReachable(t *testing.T, targetURL string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(targetURL + "/")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 500 {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func stopAndRemoveContainer(t *testing.T, ops *runtime.ContainerOps, _ string) error {
	t.Helper()
	instances, err := ops.ListContainers(context.Background())
	if err != nil {
		return err
	}
	for _, rec := range instances {
		_, _ = ops.StopContainer(context.Background(), rec.InstanceID, 5*time.Second)
		if rec.ContainerID != "" {
			_ = ops.Client().RemoveContainer(context.Background(), rec.ContainerID, true)
		}
	}
	return nil
}

var _ deployments.WorkerClient = (*realWorkerClient)(nil)
var _ deployments.WorkerClientFactory = (*realWorkerClientFactory)(nil)
