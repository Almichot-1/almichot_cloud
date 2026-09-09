package security

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nebula/nebula/internal/api"
	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

type webhookTestStack struct {
	srv         *httptest.Server
	projectRepo projects.ProjectRepository
	depService  *deployments.Service
}

func newWebhookTestStack(t *testing.T) *webhookTestStack {
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

	_, _ = reg.Register(context.Background(), workers.RegisterParams{
		WorkerKey: "webhook-worker-1",
		Hostname:  "webhook-node-1",
		Capacity:  10,
	})

	server := api.NewServer(depService, projectRepo, reg, router, log)
	srv := httptest.NewServer(api.NewRouter(server))
	t.Cleanup(srv.Close)

	return &webhookTestStack{
		srv:         srv,
		projectRepo: projectRepo,
		depService:  depService,
	}
}

// SE-03: Bad/forged webhook signature rejected (Gate G-25).
func TestSE03_BadOrForgedWebhookSignatureRejected_401(t *testing.T) {
	stack := newWebhookTestStack(t)
	ctx := context.Background()

	projectSecret := "whsec_super_secret_signing_key_42"
	_ = stack.projectRepo.Create(ctx, &projects.Project{
		ID:            "proj-webhook-test",
		Name:          "webhook-test-app",
		WebhookSecret: projectSecret,
	})

	client := &http.Client{}
	endpoint := stack.srv.URL + "/v1/projects/proj-webhook-test/webhooks"
	payload := []byte(`{"ref":"refs/heads/main","after":"abc12345","image":"webhook-app:v1"}`)

	// 1. Missing signature header -> MUST be rejected (401)
	reqMissing, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
	reqMissing.Header.Set("Content-Type", "application/json")
	respMissing, err := client.Do(reqMissing)
	if err != nil {
		t.Fatalf("missing sig request failed: %v", err)
	}
	defer respMissing.Body.Close()
	if respMissing.StatusCode != http.StatusUnauthorized {
		t.Fatalf("SE-03 failure: expected 401 for missing webhook signature, got %d", respMissing.StatusCode)
	}

	// 2. Forged signature header -> MUST be rejected (401)
	reqForged, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
	reqForged.Header.Set("Content-Type", "application/json")
	reqForged.Header.Set("X-Hub-Signature-256", "sha256=deadbeefcafebabedeadbeefcafebabedeadbeefcafebabedeadbeefcafebabe")
	respForged, err := client.Do(reqForged)
	if err != nil {
		t.Fatalf("forged sig request failed: %v", err)
	}
	defer respForged.Body.Close()
	if respForged.StatusCode != http.StatusUnauthorized {
		t.Fatalf("SE-03 failure: expected 401 for forged webhook signature, got %d", respForged.StatusCode)
	}

	// 3. Signature calculated with wrong secret key -> MUST be rejected (401)
	wrongSecretSig := auth.ComputeWebhookSignatureHeader([]byte("wrong_attacker_secret_key"), payload)
	reqWrongKey, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
	reqWrongKey.Header.Set("Content-Type", "application/json")
	reqWrongKey.Header.Set("X-Hub-Signature-256", wrongSecretSig)
	respWrongKey, err := client.Do(reqWrongKey)
	if err != nil {
		t.Fatalf("wrong key request failed: %v", err)
	}
	defer respWrongKey.Body.Close()
	if respWrongKey.StatusCode != http.StatusUnauthorized {
		t.Fatalf("SE-03 failure: expected 401 for wrong key signature, got %d", respWrongKey.StatusCode)
	}

	t.Log("SE-03 Passed: Missing, forged, and wrong-key webhook signatures are strictly rejected with 401 (Gate G-25)!")
}

// SE-04: Valid webhook signature accepted: Correctly signed webhook triggers the expected deploy.
func TestSE04_ValidWebhookSignatureAccepted_TriggersDeploy(t *testing.T) {
	stack := newWebhookTestStack(t)
	ctx := context.Background()

	projectSecret := "whsec_authentic_production_key_999"
	projectID := "proj-webhook-deploy"
	_ = stack.projectRepo.Create(ctx, &projects.Project{
		ID:            projectID,
		Name:          "authentic-deploy-app",
		WebhookSecret: projectSecret,
	})

	client := &http.Client{}
	endpoint := stack.srv.URL + "/v1/projects/" + projectID + "/webhooks"
	payload := []byte(`{"ref":"refs/heads/production","after":"commit9876","image":"authentic-app:v1","instance_count":1}`)

	// Compute authentic HMAC-SHA256 signature
	validSignature := auth.ComputeWebhookSignatureHeader([]byte(projectSecret), payload)

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hub-Signature-256", validSignature)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("valid webhook request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("SE-04 failure: expected 202 Accepted, got %d: %s", resp.StatusCode, string(body))
	}

	var depResp api.DeploymentResponse
	if err := json.NewDecoder(resp.Body).Decode(&depResp); err != nil {
		t.Fatalf("decode deployment response: %v", err)
	}

	if depResp.ID == "" {
		t.Fatalf("expected non-empty deployment ID from webhook trigger")
	}
	if depResp.ProjectID != projectID {
		t.Fatalf("expected project ID %s, got %s", projectID, depResp.ProjectID)
	}
	if depResp.Status != "RUNNING" {
		t.Fatalf("expected deployment status RUNNING, got %s", depResp.Status)
	}

	t.Logf("SE-04 Passed: Valid webhook signature accepted and triggered deployment %s successfully!", depResp.ID)
}
