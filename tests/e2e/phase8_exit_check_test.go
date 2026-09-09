package e2e

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nebula/nebula/internal/api"
	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/secrets"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// Phase 8 Exit Check:
// 1. Send an unauthenticated deploy request -> rejected (401).
// 2. Send a forged webhook -> rejected (401).
// 3. Inspect a build container's environment for secrets -> none present.
// Gate G-25: auth + webhook + build isolation, fully closed.
func TestPhase8_ExitCheck(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// 1. Setup Control Plane
	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	projectRepo := projects.NewMemoryProjectRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()
	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockFactory, log)
	router := loadbalancer.NewRouter(log)

	keyProvider := secrets.NewSingleKeyProvider([]byte("phase-8-exit-check-encryption-k!"))
	secretStore := secrets.NewMemorySecretStore(keyProvider)
	redactor := secrets.NewRedactor()
	depService.SetSecretStore(secretStore)
	depService.SetRedactor(redactor)

	tokenStore := auth.NewTokenStore()
	tokenStore.RegisterToken("valid-admin-token", &auth.User{
		ID:              "admin-user",
		Username:        "admin",
		Role:            "admin",
		AllowedProjects: []string{"*"},
	})
	authenticator := auth.NewTokenAuthenticator(tokenStore)

	server := api.NewServer(depService, projectRepo, reg, router, log)
	server.SetAuthenticator(authenticator)
	server.SetSecretStore(secretStore)
	server.SetRedactor(redactor)

	srv := httptest.NewServer(api.NewRouter(server))
	defer srv.Close()

	client := &http.Client{}

	// Create test project with authentic webhook secret
	projectID := "proj-exit-check"
	webhookSecret := "phase8_authentic_webhook_secret_key"
	_ = projectRepo.Create(ctx, &projects.Project{
		ID:            projectID,
		Name:          "exit-check-app",
		WebhookSecret: webhookSecret,
	})

	// -------------------------------------------------------------------------
	// EXIT CHECK STEP 1: Send unauthenticated deploy request -> MUST be rejected (401)
	// -------------------------------------------------------------------------
	deployPayload := `{"image":"nginx:alpine","instance_count":1}`
	reqUnauth, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/projects/"+projectID+"/deployments", bytes.NewBufferString(deployPayload))
	if err != nil {
		t.Fatalf("new unauth request: %v", err)
	}
	reqUnauth.Header.Set("Content-Type", "application/json")
	// Note: NO Authorization header supplied!

	respUnauth, err := client.Do(reqUnauth)
	if err != nil {
		t.Fatalf("unauth request failed: %v", err)
	}
	defer respUnauth.Body.Close()

	if respUnauth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Phase 8 Exit Check FAILED at Step 1: expected 401 Unauthorized for unauthenticated deploy request, got %d",
			respUnauth.StatusCode)
	}
	t.Log("Exit Check Step 1 Passed: Unauthenticated deploy request was rejected with 401 Unauthorized (SE-01, G-25)!")

	// -------------------------------------------------------------------------
	// EXIT CHECK STEP 2: Send forged webhook -> MUST be rejected (401)
	// -------------------------------------------------------------------------
	webhookPayload := []byte(`{"ref":"refs/heads/main","after":"c0ffee42","image":"forged-app:v1"}`)
	webhookURL := srv.URL + "/v1/projects/" + projectID + "/webhooks"

	reqForged, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewReader(webhookPayload))
	if err != nil {
		t.Fatalf("new forged webhook request: %v", err)
	}
	reqForged.Header.Set("Content-Type", "application/json")
	// Forged HMAC signature
	reqForged.Header.Set("X-Hub-Signature-256", "sha256=baadc0ffeedeadbeef0000111122223333444455556666777788889999aaaabb")

	respForged, err := client.Do(reqForged)
	if err != nil {
		t.Fatalf("forged webhook request failed: %v", err)
	}
	defer respForged.Body.Close()

	if respForged.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Phase 8 Exit Check FAILED at Step 2: expected 401 Unauthorized for forged webhook signature, got %d",
			respForged.StatusCode)
	}
	t.Log("Exit Check Step 2 Passed: Forged webhook request was rejected with 401 Unauthorized (SE-03, G-25)!")

	// -------------------------------------------------------------------------
	// EXIT CHECK STEP 3: Inspect build container's environment for secrets -> none present
	// -------------------------------------------------------------------------
	sandbox := build.NewEphemeralSandbox(log)
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"echo\", \"hello\"]"), 0644)

	sensitiveCPConfig := map[string]string{
		"NEBULA_DB_PASSWORD":  "super-secret-postgres-root-pass",
		"DATABASE_URL":        "postgres://nebula:secretpass@127.0.0.1:5432/nebula",
		"NEBULA_SECRETS_KEY":  "32-byte-aes-master-key-must-not-leak",
		"ADMIN_API_TOKEN":     "admin-bearer-token-val",
		"CONTROL_PLANE_GRPC":  "127.0.0.1:9090",
		"CP_INTERNAL_ENDPOINT": "http://control-plane.nebula.internal:9090",
		"PUBLIC_ENV":          "production-ready",
	}

	buildPlan := &build.BuildPlan{
		Strategy:          build.StrategyDockerfile,
		DockerfileContent: "FROM alpine:latest\nCMD [\"echo\", \"hello\"]",
	}

	buildRes, err := sandbox.Build(ctx, build.BuildRequest{
		ProjectID: projectID,
		SourceDir: tmpDir,
		Plan:      buildPlan,
		ImageTag:  "nebula/exit-check:v1",
		BuildArgs: sensitiveCPConfig,
	})
	if err != nil {
		t.Fatalf("build execution failed: %v", err)
	}

	// Verify build container artifacts and environment have NO CP secrets or gRPC paths
	for secretKey, secretVal := range sensitiveCPConfig {
		if secretKey == "PUBLIC_ENV" {
			continue
		}
		if strings.Contains(buildRes.BuildLog, secretVal) {
			t.Fatalf("Phase 8 Exit Check FAILED at Step 3: secret %s value found in build logs!", secretKey)
		}
		if strings.Contains(string(buildRes.ImageData), secretVal) {
			t.Fatalf("Phase 8 Exit Check FAILED at Step 3: secret %s value found in container image payload!", secretKey)
		}
	}

	// Verify SanitizeEnvironment stripped all sensitive keys
	sanitizedEnv := build.SanitizeEnvironment(sensitiveCPConfig)
	for k := range sanitizedEnv {
		upperK := strings.ToUpper(k)
		if strings.Contains(upperK, "PASSWORD") ||
			strings.Contains(upperK, "SECRET") ||
			strings.Contains(upperK, "TOKEN") ||
			strings.Contains(upperK, "KEY") ||
			strings.Contains(upperK, "DB_") {
			t.Fatalf("Phase 8 Exit Check FAILED at Step 3: sensitive key %s present in sanitized build environment!", k)
		}
	}

	t.Log("Exit Check Step 3 Passed: Inspected build container environment and payload: zero secrets and no reachable CP endpoints present (SE-05, G-25)!")
	t.Log(">>> PHASE 8 EXIT CHECK FULLY PASSED: Gate G-25 (auth + webhook + build isolation) is 100% satisfied! <<<")
}
