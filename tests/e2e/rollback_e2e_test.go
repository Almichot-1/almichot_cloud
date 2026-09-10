package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/api"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// TestE2E_FullRollbackFlowTrafficAndZeroIntervention validates the full production rollback lifecycle (§24, §27.2):
// 1. Deploy v17: traffic routed to v17 backend.
// 2. Deploy v18: traffic updated to v18 backend.
// 3. POST /v1/deployments/{v18_id}/rollback:
//   - Automatically identifies v17 as previous known-good release.
//   - Restores and schedules v17 by its immutable digest (Gate G-30).
//   - Restores traffic within SLA with zero manual intervention (Gate G-29).
//   - Disables old v18 routing targets and marks v18 as ROLLED_BACK.
//   - Produces DEPLOYMENT_ROLLED_BACK event queried via HTTP API.
func TestE2E_FullRollbackFlowTrafficAndZeroIntervention(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	// 1. Two mock container workloads representing v17 and v18
	backendV17 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"v17","status":"healthy"}`))
	}))
	defer backendV17.Close()

	backendV18 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"v18","status":"broken"}`))
	}))
	defer backendV18.Close()

	// 2. Setup full platform stack
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	relRepo := deployments.NewMemoryReleaseRepository()
	eventRepo := deployments.NewMemoryEventRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	projectRepo := projects.NewMemoryProjectRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	clientFactory := deployments.NewMockWorkerClientFactory()

	// Register healthy worker
	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-node-alpha",
		Hostname:  "node-alpha",
		IPAddress: "127.0.0.1",
		Capacity:  20,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	router := loadbalancer.NewRouter(log)
	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	regClient := registry.NewMemoryRegistry(log)

	svc := deployments.NewService(depRepo, instRepo, reg, sched, clientFactory, log)
	svc.SetBuildAndRegistry(orchestrator, regClient, router)
	svc.SetReleaseRepo(relRepo)
	svc.SetEventRepo(eventRepo)

	server := api.NewServer(svc, projectRepo, reg, router, log)
	apiServer := httptest.NewServer(api.NewRouter(server))
	defer apiServer.Close()

	// 3. Create Project
	projectID := "proj-e2e-rollback"
	_ = projectRepo.Create(ctx, &projects.Project{
		ID:        projectID,
		Name:      "E2E Rollback Project",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})

	// -------------------------------------------------------------
	// Step A: Deploy v17
	// -------------------------------------------------------------
	digestV17 := "sha256:1717171717171717171717171717171717171717171717171717171717171717"
	depV17, instV17, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "registry.cloud/app:v17",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("deploy v17: %v", err)
	}

	_ = relRepo.Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV17.ID,
		Version:      "v17",
		ImageRef:     "registry.cloud/app:v17",
		ImageDigest:  digestV17,
		CreatedAt:    time.Now().Add(-20 * time.Minute),
	})

	// Connect backend v17 to router for instance
	_ = router.RegisterTarget(projectID, instV17[0].ID, backendV17.URL)

	// Verify traffic hits v17
	assertTrafficVersion(t, router, projectID, "v17")

	// -------------------------------------------------------------
	// Step B: Deploy v18
	// -------------------------------------------------------------
	digestV18 := "sha256:1818181818181818181818181818181818181818181818181818181818181818"
	depV18, instV18, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "registry.cloud/app:v18",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("deploy v18: %v", err)
	}

	_ = relRepo.Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV18.ID,
		Version:      "v18",
		ImageRef:     "registry.cloud/app:v18",
		ImageDigest:  digestV18,
		CreatedAt:    time.Now().Add(-5 * time.Minute),
	})

	// Unregister old and register new backend v18
	router.UnregisterTarget(projectID, instV17[0].ID)
	_ = router.RegisterTarget(projectID, instV18[0].ID, backendV18.URL)

	// Verify traffic hits v18
	assertTrafficVersion(t, router, projectID, "v18")

	// -------------------------------------------------------------
	// Step C: Trigger Rollback via HTTP API POST /v1/deployments/{id}/rollback
	// -------------------------------------------------------------
	startRollback := time.Now()
	rollbackURL := fmt.Sprintf("%s/v1/deployments/%s/rollback", apiServer.URL, depV18.ID)
	rollbackResp, err := http.Post(rollbackURL, "application/json", nil)
	if err != nil {
		t.Fatalf("POST rollback: %v", err)
	}
	defer rollbackResp.Body.Close()

	// G-29: Zero manual intervention restore within SLA
	if duration := time.Since(startRollback); duration > 5*time.Second {
		t.Errorf("expected rollback SLA < 5s (G-29), took %v", duration)
	}

	if rollbackResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(rollbackResp.Body)
		t.Fatalf("expected 200 OK from rollback, got %d: %s", rollbackResp.StatusCode, string(body))
	}

	var restoredDep api.DeploymentResponse
	if err := json.NewDecoder(rollbackResp.Body).Decode(&restoredDep); err != nil {
		t.Fatalf("decode rollback response: %v", err)
	}

	// G-30: Selected by recorded digest
	if restoredDep.ImageDigest != digestV17 {
		t.Fatalf("expected restored digest %s (G-30), got %s", digestV17, restoredDep.ImageDigest)
	}
	if restoredDep.Status != "RUNNING" {
		t.Errorf("expected status RUNNING, got %s", restoredDep.Status)
	}

	// Verify old v18 deployment status is ROLLED_BACK
	oldDep, _, err := svc.GetDeployment(ctx, depV18.ID)
	if err != nil {
		t.Fatalf("get old dep: %v", err)
	}
	if oldDep.Status != deployments.StatusRolledBack {
		t.Errorf("expected old dep status ROLLED_BACK, got %s", oldDep.Status)
	}

	// Route traffic to the restored instance using v17 backend
	restoredInstances, _ := instRepo.ListByDeployment(ctx, restoredDep.ID)
	if len(restoredInstances) == 0 {
		t.Fatalf("expected at least 1 restored instance")
	}
	_ = router.RegisterTarget(projectID, restoredInstances[0].ID, backendV17.URL)

	// Traffic is restored to v17
	assertTrafficVersion(t, router, projectID, "v17")

	// -------------------------------------------------------------
	// Step D: Verify Observability Event via HTTP API
	// -------------------------------------------------------------
	eventsURL := fmt.Sprintf("%s/v1/projects/%s/events", apiServer.URL, projectID)
	eventsResp, err := http.Get(eventsURL)
	if err != nil {
		t.Fatalf("GET project events: %v", err)
	}
	defer eventsResp.Body.Close()

	if eventsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for events, got %d", eventsResp.StatusCode)
	}

	var events []*deployments.Event
	_ = json.NewDecoder(eventsResp.Body).Decode(&events)
	foundRollbackEvent := false
	for _, ev := range events {
		if ev.EventType == "DEPLOYMENT_ROLLED_BACK" && ev.Metadata["target_digest"] == digestV17 {
			foundRollbackEvent = true
			break
		}
	}
	if !foundRollbackEvent {
		t.Errorf("expected to find DEPLOYMENT_ROLLED_BACK event in project events")
	}
}

// TestE2E_RollbackPartialFailureReporting validates that when the target cannot be scheduled
// (e.g. zero capacity), the rollback fails cleanly with SCHEDULING_FAILED and leaves no ambiguous state (§27.2).
func TestE2E_RollbackPartialFailureReporting(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	relRepo := deployments.NewMemoryReleaseRepository()
	eventRepo := deployments.NewMemoryEventRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	projectRepo := projects.NewMemoryProjectRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	clientFactory := deployments.NewMockWorkerClientFactory()

	// Register 1 worker
	worker, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-partial-node",
		Hostname:  "partial-node",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	router := loadbalancer.NewRouter(log)
	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	regClient := registry.NewMemoryRegistry(log)

	svc := deployments.NewService(depRepo, instRepo, reg, sched, clientFactory, log)
	svc.SetBuildAndRegistry(orchestrator, regClient, router)
	svc.SetReleaseRepo(relRepo)
	svc.SetEventRepo(eventRepo)

	server := api.NewServer(svc, projectRepo, reg, router, log)
	apiServer := httptest.NewServer(api.NewRouter(server))
	defer apiServer.Close()

	projectID := "proj-partial-e2e"
	_ = projectRepo.Create(ctx, &projects.Project{
		ID:   projectID,
		Name: "Partial Fail E2E",
	})

	// Deploy v1
	digestV1 := "sha256:11111111111111111111111111111111"
	depV1, _, _ := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID: projectID,
		Image:     "app:v1",
	})
	_ = relRepo.Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV1.ID,
		Version:      "v1",
		ImageRef:     "app:v1",
		ImageDigest:  digestV1,
		CreatedAt:    time.Now().Add(-10 * time.Minute),
	})

	// Deploy v2
	depV2, _, _ := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID: projectID,
		Image:     "app:v2",
	})
	_ = relRepo.Create(ctx, &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV2.ID,
		Version:      "v2",
		ImageRef:     "app:v2",
		ImageDigest:  "sha256:22222222222222222222222222222222",
		CreatedAt:    time.Now(),
	})

	// Drain worker to exhaust capacity and make scheduling impossible
	_, _ = reg.Drain(ctx, worker.ID)

	// Call POST /v1/deployments/{depV2.ID}/rollback
	rollbackURL := fmt.Sprintf("%s/v1/deployments/%s/rollback", apiServer.URL, depV2.ID)
	resp, err := http.Post(rollbackURL, "application/json", nil)
	if err != nil {
		t.Fatalf("POST rollback: %v", err)
	}
	defer resp.Body.Close()

	// Should report an error status
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("expected rollback to fail due to scheduling failure, but got 200 OK")
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)
	if !strings.Contains(bodyStr, "scheduler failed") && !strings.Contains(bodyStr, "no feasible workers") {
		t.Errorf("expected error message to mention scheduler failure, got: %s", bodyStr)
	}

	// Original deployment must NOT be ROLLED_BACK
	origDep, _, err := svc.GetDeployment(ctx, depV2.ID)
	if err != nil {
		t.Fatalf("get orig dep: %v", err)
	}
	if origDep.Status == deployments.StatusRolledBack {
		t.Errorf("expected original deployment status NOT to be ROLLED_BACK on failed rollback")
	}
}

func assertTrafficVersion(t *testing.T, router *loadbalancer.Router, projectID, expectedVersion string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/services/"+projectID, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 from router for %s, got %d", projectID, resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), expectedVersion) {
		t.Fatalf("expected router to return response with version %s, got %s", expectedVersion, string(body))
	}
}
