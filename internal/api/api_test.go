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
	"time"

	"github.com/nebula/nebula/internal/autoscaler"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

type testStack struct {
	srv         *httptest.Server
	apiServer   *Server
	registry    *workers.Registry
	depRepo     deployments.DeploymentRepository
	instRepo    deployments.InstanceRepository
	projectRepo projects.ProjectRepository
	svc         *deployments.Service
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

	apiServer := NewServer(depService, projectRepo, reg, router, log)
	srv := httptest.NewServer(NewRouter(apiServer))
	t.Cleanup(srv.Close)

	return &testStack{srv: srv, apiServer: apiServer, registry: reg, depRepo: depRepo, instRepo: instRepo, projectRepo: projectRepo, svc: depService}
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

func TestHTTP_RollbackEndpoint(t *testing.T) {
	stack := newTestStack(t)
	ctx := context.Background()

	// Register healthy worker
	_, err := stack.registry.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-http-1",
		Hostname:  "node-http-1",
		IPAddress: "127.0.0.1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	projectID := mustCreateProject(t, stack.srv, "proj-rollback-api")

	// 1. Deploy v1
	depV1Resp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/deployments", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(`{"image":"app:v1","instance_count":1}`),
	)
	if err != nil {
		t.Fatalf("deploy v1: %v", err)
	}
	defer depV1Resp.Body.Close()
	var v1Body DeploymentResponse
	_ = json.NewDecoder(depV1Resp.Body).Decode(&v1Body)

	// Attach release v1 record
	digestV1 := "sha256:v1-valid-digest"
	_ = stack.svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           "rel-1",
		ProjectID:    projectID,
		DeploymentID: v1Body.ID,
		Version:      "v1",
		ImageRef:     "app:v1",
		ImageDigest:  digestV1,
		CreatedAt:    time.Now().Add(-5 * time.Minute),
	})

	// 2. Deploy v2
	depV2Resp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/deployments", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(`{"image":"app:v2","instance_count":1}`),
	)
	if err != nil {
		t.Fatalf("deploy v2: %v", err)
	}
	defer depV2Resp.Body.Close()
	var v2Body DeploymentResponse
	_ = json.NewDecoder(depV2Resp.Body).Decode(&v2Body)

	// Attach release v2 record
	digestV2 := "sha256:v2-bad-digest"
	_ = stack.svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           "rel-2",
		ProjectID:    projectID,
		DeploymentID: v2Body.ID,
		Version:      "v2",
		ImageRef:     "app:v2",
		ImageDigest:  digestV2,
		CreatedAt:    time.Now(),
	})

	// 3. Trigger Rollback on v2 via POST /v1/deployments/{id}/rollback
	rollbackResp, err := http.Post(
		fmt.Sprintf("%s/v1/deployments/%s/rollback", stack.srv.URL, v2Body.ID),
		"application/json",
		nil,
	)
	if err != nil {
		t.Fatalf("POST rollback: %v", err)
	}
	defer rollbackResp.Body.Close()

	if rollbackResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(rollbackResp.Body)
		t.Fatalf("expected status 200 OK, got %d: %s", rollbackResp.StatusCode, string(body))
	}

	rolledBackHeader := rollbackResp.Header.Get("X-Rolled-Back-Deployment-ID")
	if rolledBackHeader != v2Body.ID {
		t.Errorf("expected X-Rolled-Back-Deployment-ID header %s, got %s", v2Body.ID, rolledBackHeader)
	}

	var rollbackResult DeploymentResponse
	if err := json.NewDecoder(rollbackResp.Body).Decode(&rollbackResult); err != nil {
		t.Fatalf("decode rollback response: %v", err)
	}

	if rollbackResult.ImageDigest != digestV1 {
		t.Errorf("expected restored deployment to have digest %s, got %s", digestV1, rollbackResult.ImageDigest)
	}
	if rollbackResult.Status != "RUNNING" {
		t.Errorf("expected restored deployment status RUNNING, got %s", rollbackResult.Status)
	}

	// 4. Verify GET /v1/deployments/{v2_id} returns ROLLED_BACK
	getV2Resp, err := http.Get(fmt.Sprintf("%s/v1/deployments/%s", stack.srv.URL, v2Body.ID))
	if err != nil {
		t.Fatalf("GET v2 deployment: %v", err)
	}
	defer getV2Resp.Body.Close()

	var getV2Body DeploymentResponse
	_ = json.NewDecoder(getV2Resp.Body).Decode(&getV2Body)
	if getV2Body.Status != "ROLLED_BACK" {
		t.Errorf("expected v2 status ROLLED_BACK, got %s", getV2Body.Status)
	}
}

func TestHTTP_StopEndpoint(t *testing.T) {
	stack := newTestStack(t)
	ctx := context.Background()

	_, _ = stack.registry.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-http-stop",
		Hostname:  "node-stop",
		IPAddress: "127.0.0.1",
		Capacity:  10,
	})

	projectID := mustCreateProject(t, stack.srv, "proj-stop-api")

	depResp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/deployments", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(`{"image":"app:v1","instance_count":1}`),
	)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	defer depResp.Body.Close()
	var dep DeploymentResponse
	_ = json.NewDecoder(depResp.Body).Decode(&dep)

	// Call POST /v1/deployments/{id}/stop
	stopResp, err := http.Post(
		fmt.Sprintf("%s/v1/deployments/%s/stop", stack.srv.URL, dep.ID),
		"application/json",
		nil,
	)
	if err != nil {
		t.Fatalf("stop request: %v", err)
	}
	defer stopResp.Body.Close()

	if stopResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(stopResp.Body)
		t.Fatalf("expected 200 OK from stop, got %d: %s", stopResp.StatusCode, string(body))
	}

	var stoppedBody DeploymentResponse
	_ = json.NewDecoder(stopResp.Body).Decode(&stoppedBody)
	if stoppedBody.Status != "STOPPED" {
		t.Errorf("expected status STOPPED, got %s", stoppedBody.Status)
	}
}

func TestHTTP_EventsEndpoints(t *testing.T) {
	stack := newTestStack(t)
	ctx := context.Background()

	_, _ = stack.registry.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-http-events",
		Hostname:  "node-events",
		IPAddress: "127.0.0.1",
		Capacity:  10,
	})

	projectID := mustCreateProject(t, stack.srv, "proj-events-api")

	// 1. Initial deployment
	depResp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/deployments", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(`{"image":"app:v1","instance_count":1}`),
	)
	if err != nil {
		t.Fatalf("deploy v1: %v", err)
	}
	defer depResp.Body.Close()
	var dep1 DeploymentResponse
	_ = json.NewDecoder(depResp.Body).Decode(&dep1)

	// Attach release record
	_ = stack.svc.ReleaseRepo().Create(ctx, &deployments.Release{
		ID:           "rel-1",
		ProjectID:    projectID,
		DeploymentID: dep1.ID,
		Version:      "v1",
		ImageRef:     "app:v1",
		ImageDigest:  "sha256:11111111111111111111111111111111",
		CreatedAt:    time.Now().Add(-5 * time.Minute),
	})

	// 2. Second deployment
	dep2Resp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/deployments", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(`{"image":"app:v2","instance_count":1}`),
	)
	if err != nil {
		t.Fatalf("deploy v2: %v", err)
	}
	defer dep2Resp.Body.Close()
	var dep2 DeploymentResponse
	_ = json.NewDecoder(dep2Resp.Body).Decode(&dep2)

	// 3. Trigger rollback of dep2 -> generates DEPLOYMENT_ROLLED_BACK event
	rollbackResp, err := http.Post(
		fmt.Sprintf("%s/v1/deployments/%s/rollback", stack.srv.URL, dep2.ID),
		"application/json",
		nil,
	)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	defer rollbackResp.Body.Close()
	if rollbackResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(rollbackResp.Body)
		t.Fatalf("rollback failed: %d: %s", rollbackResp.StatusCode, string(body))
	}

	// 4. Query GET /v1/projects/{projectID}/events
	eventsResp, err := http.Get(fmt.Sprintf("%s/v1/projects/%s/events", stack.srv.URL, projectID))
	if err != nil {
		t.Fatalf("GET project events: %v", err)
	}
	defer eventsResp.Body.Close()

	if eventsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for project events, got %d", eventsResp.StatusCode)
	}

	var pEvents []*deployments.Event
	if err := json.NewDecoder(eventsResp.Body).Decode(&pEvents); err != nil {
		t.Fatalf("decode project events: %v", err)
	}

	if len(pEvents) == 0 {
		t.Errorf("expected at least 1 event for project %s, got 0", projectID)
	}

	// 5. Query GET /v1/deployments/{id}/events
	depEventsResp, err := http.Get(fmt.Sprintf("%s/v1/deployments/%s/events", stack.srv.URL, dep2.ID))
	if err != nil {
		t.Fatalf("GET deployment events: %v", err)
	}
	defer depEventsResp.Body.Close()

	if depEventsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for deployment events, got %d", depEventsResp.StatusCode)
	}

	var dEvents []*deployments.Event
	if err := json.NewDecoder(depEventsResp.Body).Decode(&dEvents); err != nil {
		t.Fatalf("decode deployment events: %v", err)
	}

	if len(dEvents) == 0 {
		t.Errorf("expected at least 1 event for deployment %s, got 0", dep2.ID)
	}
	foundRollbackEvent := false
	for _, ev := range dEvents {
		if ev.EventType == "DEPLOYMENT_ROLLED_BACK" {
			foundRollbackEvent = true
			break
		}
	}
	if !foundRollbackEvent {
		t.Errorf("expected to find DEPLOYMENT_ROLLED_BACK event in deployment events")
	}

	// 6. Non-existent deployment returns 404
	badResp, err := http.Get(fmt.Sprintf("%s/v1/deployments/non-existent-id/events", stack.srv.URL))
	if err != nil {
		t.Fatalf("GET bad deployment: %v", err)
	}
	defer badResp.Body.Close()
	if badResp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for bad deployment id, got %d", badResp.StatusCode)
	}
}

func TestHTTP_ScaleDeploymentAndProject(t *testing.T) {
	stack := newTestStack(t)
	ctx := context.Background()

	// Register 2 healthy workers
	for i := 1; i <= 2; i++ {
		_, err := stack.registry.Register(ctx, workers.RegisterParams{
			WorkerKey: fmt.Sprintf("worker-scale-%d", i),
			Hostname:  fmt.Sprintf("node-scale-%d", i),
			IPAddress: "127.0.0.1",
			Capacity:  10,
		})
		if err != nil {
			t.Fatalf("register worker: %v", err)
		}
	}

	projectID := mustCreateProject(t, stack.srv, "proj-scale-api")

	// 1. Deploy with initial 1 instance
	depResp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/deployments", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(`{"image":"app:v1","instance_count":1}`),
	)
	if err != nil {
		t.Fatalf("deploy initial: %v", err)
	}
	defer depResp.Body.Close()
	var initialDep DeploymentResponse
	if err := json.NewDecoder(depResp.Body).Decode(&initialDep); err != nil {
		t.Fatalf("decode initial dep: %v", err)
	}
	if initialDep.InstanceCount != 1 {
		t.Fatalf("expected 1 instance initially, got %d", initialDep.InstanceCount)
	}

	// 2. Scale deployment up to 3 via POST /v1/deployments/{id}/scale
	scaleDepResp, err := http.Post(
		fmt.Sprintf("%s/v1/deployments/%s/scale", stack.srv.URL, initialDep.ID),
		"application/json",
		strings.NewReader(`{"desired_replicas":3}`),
	)
	if err != nil {
		t.Fatalf("scale deployment POST: %v", err)
	}
	defer scaleDepResp.Body.Close()

	if scaleDepResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(scaleDepResp.Body)
		t.Fatalf("expected 200 OK from scale deployment, got %d body: %s", scaleDepResp.StatusCode, string(body))
	}

	var scaledDep DeploymentResponse
	if err := json.NewDecoder(scaleDepResp.Body).Decode(&scaledDep); err != nil {
		t.Fatalf("decode scaled dep: %v", err)
	}
	if scaledDep.InstanceCount != 3 || scaledDep.DesiredReplicas != 3 {
		t.Errorf("expected 3 replicas after scale up, got count=%d desired=%d", scaledDep.InstanceCount, scaledDep.DesiredReplicas)
	}
	if len(scaledDep.Instances) != 3 {
		t.Errorf("expected 3 instances returned, got %d", len(scaledDep.Instances))
	}

	// 3. Scale project down to 2 via POST /v1/projects/{projectID}/scale
	scaleProjResp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/scale", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(`{"desired_replicas":2}`),
	)
	if err != nil {
		t.Fatalf("scale project POST: %v", err)
	}
	defer scaleProjResp.Body.Close()

	if scaleProjResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(scaleProjResp.Body)
		t.Fatalf("expected 200 OK from scale project, got %d body: %s", scaleProjResp.StatusCode, string(body))
	}

	var scaledProjDep DeploymentResponse
	if err := json.NewDecoder(scaleProjResp.Body).Decode(&scaledProjDep); err != nil {
		t.Fatalf("decode scaled proj dep: %v", err)
	}
	if scaledProjDep.InstanceCount != 2 || scaledProjDep.DesiredReplicas != 2 {
		t.Errorf("expected 2 replicas after scale down, got count=%d desired=%d", scaledProjDep.InstanceCount, scaledProjDep.DesiredReplicas)
	}
	if len(scaledProjDep.Instances) != 2 {
		t.Errorf("expected 2 instances returned, got %d", len(scaledProjDep.Instances))
	}
}

func TestHTTP_AutoscalerEndpoints(t *testing.T) {
	stack := newTestStack(t)
	ctx := context.Background()

	// Register worker
	_, err := stack.registry.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-as-1",
		Hostname:  "node-as-1",
		IPAddress: "127.0.0.1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	projectID := mustCreateProject(t, stack.srv, "proj-as-api")

	// Deploy initial 1 instance
	depResp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/deployments", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(`{"image":"app:v1","instance_count":1}`),
	)
	if err != nil {
		t.Fatalf("deploy initial: %v", err)
	}
	_ = depResp.Body.Close()

	// Setup autoscaler with MemoryMetricsProvider
	metricsProvider := autoscaler.NewMemoryMetricsProvider()
	as := autoscaler.NewAutoscaler(metricsProvider, stack.svc, stack.projectRepo, stack.svc.EventRepo(), zerolog.Nop())
	stack.apiServer.SetAutoscaler(as)

	// 1. Set policy via POST /v1/projects/{projectID}/autoscaler/policy
	policyPayload := `{
		"metric_type": "cpu",
		"target_value": 70,
		"min_replicas": 1,
		"max_replicas": 5,
		"scale_up_step": 1,
		"scale_down_step": 1,
		"cooldown_seconds": 60,
		"tolerance": 0.1
	}`
	setPolicyResp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/autoscaler/policy", stack.srv.URL, projectID),
		"application/json",
		strings.NewReader(policyPayload),
	)
	if err != nil {
		t.Fatalf("set policy: %v", err)
	}
	defer setPolicyResp.Body.Close()
	if setPolicyResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(setPolicyResp.Body)
		t.Fatalf("expected 200 OK from set policy, got %d body: %s", setPolicyResp.StatusCode, string(body))
	}

	// 2. Query policy via GET /v1/projects/{projectID}/autoscaler/policy
	getPolicyResp, err := http.Get(fmt.Sprintf("%s/v1/projects/%s/autoscaler/policy", stack.srv.URL, projectID))
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	defer getPolicyResp.Body.Close()
	if getPolicyResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from get policy, got %d", getPolicyResp.StatusCode)
	}
	var fetchedPolicy autoscaler.ScalingPolicy
	if err := json.NewDecoder(getPolicyResp.Body).Decode(&fetchedPolicy); err != nil {
		t.Fatalf("decode policy: %v", err)
	}
	if fetchedPolicy.TargetValue != 70 || fetchedPolicy.MaxReplicas != 5 {
		t.Errorf("unexpected policy values: target=%f max=%d", fetchedPolicy.TargetValue, fetchedPolicy.MaxReplicas)
	}

	// 3. Set metric to 85% CPU (above target 70) and trigger evaluate
	metricsProvider.SetMetric(projectID, autoscaler.MetricCPU, 85)

	evalResp, err := http.Post(
		fmt.Sprintf("%s/v1/projects/%s/autoscaler/evaluate", stack.srv.URL, projectID),
		"application/json",
		nil,
	)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	defer evalResp.Body.Close()
	if evalResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(evalResp.Body)
		t.Fatalf("expected 200 OK from evaluate, got %d body: %s", evalResp.StatusCode, string(body))
	}

	var decision autoscaler.ScalingDecision
	if err := json.NewDecoder(evalResp.Body).Decode(&decision); err != nil {
		t.Fatalf("decode decision: %v", err)
	}
	if decision.Action != "SCALE_UP" || decision.DesiredReplicas != 2 {
		t.Errorf("expected SCALE_UP to 2 replicas, got action=%s desired=%d reason=%s", decision.Action, decision.DesiredReplicas, decision.Reason)
	}
}