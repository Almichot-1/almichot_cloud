package security

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/secrets"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// SEC-01: Secrets encrypted at rest.
// Raw storage record contains ciphertext only, never plaintext.
func TestSEC01_SecretsEncryptedAtRest(t *testing.T) {
	ctx := context.Background()
	rawMasterKey := []byte("01234567890123456789012345678901") // 32 bytes
	keyProvider := secrets.NewSingleKeyProvider(rawMasterKey)
	store := secrets.NewMemorySecretStore(keyProvider)

	projectID := "proj-sec01"
	secretName := "DATABASE_PASSWORD"
	plaintext := "my-super-secret-production-password-xyz987"

	sec, err := store.SetSecret(ctx, projectID, secretName, plaintext)
	if err != nil {
		t.Fatalf("set secret: %v", err)
	}

	// 1. Verify that raw ciphertext DOES NOT contain plaintext
	if bytes.Contains(sec.Ciphertext, []byte(plaintext)) {
		t.Fatalf("SEC-01 violation: raw storage ciphertext contains plaintext!")
	}
	if strings.Contains(string(sec.Ciphertext), plaintext) {
		t.Fatalf("SEC-01 violation: plaintext leaked into ciphertext string representation!")
	}

	// 2. Verify AES-GCM decryption with correct key succeeds
	retrievedSec, decrypted, err := store.GetSecret(ctx, projectID, secretName)
	if err != nil {
		t.Fatalf("get secret failed: %v", err)
	}
	if decrypted != plaintext {
		t.Fatalf("expected decrypted plaintext %q, got %q", plaintext, decrypted)
	}
	if retrievedSec.KeyVersion != 1 {
		t.Fatalf("expected key version 1, got %d", retrievedSec.KeyVersion)
	}

	// 3. Verify decryption fails with wrong key
	wrongKeyProvider := secrets.NewSingleKeyProvider([]byte("wrong-key-0000000000000000000000"))
	wrongStore := secrets.NewMemorySecretStore(wrongKeyProvider)
	_, _, err = wrongStore.GetSecret(ctx, projectID, secretName)
	if err == nil {
		t.Fatalf("expected error when querying non-existent secret in wrong store")
	}

	// Directly attempt decryption of ciphertext with invalid key
	wrongKey, _ := wrongKeyProvider.GetKey(1)
	_, decryptErr := secrets.Decrypt(sec.Ciphertext, wrongKey)
	if decryptErr == nil {
		t.Fatalf("SEC-01 violation: decryption with invalid key succeeded when it must fail!")
	}

	t.Log("SEC-01 Passed: Secrets are envelope-encrypted (AES-256-GCM) at rest; raw storage holds ciphertext only!")
}

// SEC-02: Secret injected at RunContainer time only.
// Secret value is absent from the image, present only in the running container's env.
func TestSEC02_SecretInjectedAtRunContainerTimeOnly(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockFactory, log)
	router := loadbalancer.NewRouter(log)
	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	depService.SetBuildAndRegistry(orchestrator, depService.RegistryClient(), router)

	keyProvider := secrets.NewSingleKeyProvider([]byte("32-byte-encryption-key-for-test!"))
	secretStore := secrets.NewMemorySecretStore(keyProvider)
	redactor := secrets.NewRedactor()
	depService.SetSecretStore(secretStore)
	depService.SetRedactor(redactor)

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "sec02-worker",
		Hostname:  "sec02-node",
		Capacity:  10,
	})

	projectID := "proj-sec02"
	secretVal := "secret-jwt-signing-key-998811"
	_, err := secretStore.SetSecret(ctx, projectID, "API_SIGNING_KEY", secretVal)
	if err != nil {
		t.Fatalf("set secret: %v", err)
	}

	// Prepare source code directory
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module secapp\ngo 1.22"), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package main\nfunc main(){}"), 0644)

	// Deploy from source
	dep, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		SourcePath:    tmpDir,
		InstanceCount: 1,
		Env: map[string]string{
			"PUBLIC_HOST": "app.nebula.internal",
		},
	})
	if err != nil {
		t.Fatalf("create and deploy: %v", err)
	}

	if dep.Status != deployments.StatusRunning {
		t.Fatalf("expected deployment RUNNING, got %s", dep.Status)
	}

	// 1. Verify secret is NOT in build image or registry
	regData, _, err := depService.RegistryClient().Pull(ctx, dep.Image)
	if err != nil {
		t.Fatalf("pull image from registry: %v", err)
	}
	if strings.Contains(string(regData), secretVal) {
		t.Fatalf("SEC-02 violation: secret value leaked into container image artifact!")
	}

	// 2. Verify secret WAS injected into worker RunContainerRequest
	if len(mockFactory.Dispatched) == 0 {
		t.Fatalf("expected container to be dispatched to worker")
	}
	dispatchedReq := mockFactory.Dispatched[0]
	if dispatchedReq.Env["API_SIGNING_KEY"] != secretVal {
		t.Fatalf("SEC-02 failure: secret not injected into container runtime env (got %q, want %q)",
			dispatchedReq.Env["API_SIGNING_KEY"], secretVal)
	}
	if dispatchedReq.Env["PUBLIC_HOST"] != "app.nebula.internal" {
		t.Fatalf("expected non-secret env to be preserved")
	}

	t.Log("SEC-02 Passed: Secret value strictly absent from container image; injected solely at RunContainer time!")
}

// SEC-03: Secret values redacted from logs and events.
// Grep of logs/events for known secret value returns nothing.
func TestSEC03_SecretValuesRedactedFromLogsAndEvents(t *testing.T) {
	redactor := secrets.NewRedactor()

	sensitiveSecret := "super-confidential-token-8877"
	databasePassword := "db-admin-pass-xyz-4433"

	redactor.Register(sensitiveSecret)
	redactor.Register(databasePassword)

	// 1. Direct string and byte redaction
	sampleLogMsg := "Deployment started with token=" + sensitiveSecret + " and db=" + databasePassword
	redactedMsg := redactor.Redact(sampleLogMsg)

	if strings.Contains(redactedMsg, sensitiveSecret) {
		t.Fatalf("SEC-03 violation: secret %s found in redacted message: %s", sensitiveSecret, redactedMsg)
	}
	if strings.Contains(redactedMsg, databasePassword) {
		t.Fatalf("SEC-03 violation: password %s found in redacted message: %s", databasePassword, redactedMsg)
	}
	if !strings.Contains(redactedMsg, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] placeholder in message, got: %s", redactedMsg)
	}

	// 2. RedactingWriter wrapping an output stream
	var logBuffer bytes.Buffer
	redactingWriter := secrets.NewRedactingWriter(&logBuffer, redactor)

	logger := zerolog.New(redactingWriter).With().Timestamp().Logger()

	logger.Info().
		Str("component", "deployer").
		Str("secret_token", sensitiveSecret).
		Str("db_password", databasePassword).
		Msg("Dispatched container with sensitive configuration")

	logOutput := logBuffer.String()

	// 3. Full grep audit of log stream
	if strings.Contains(logOutput, sensitiveSecret) {
		t.Fatalf("SEC-03 violation: sensitive token leaked in log stream: %s", logOutput)
	}
	if strings.Contains(logOutput, databasePassword) {
		t.Fatalf("SEC-03 violation: database password leaked in log stream: %s", logOutput)
	}

	t.Logf("SEC-03 Passed: Grep of log buffer for sensitive secrets returned 0 matches; sanitized to:\n%s", logOutput)
}

// SEC-04: Secret rotation creates a new version without breaking running instances.
// In-flight containers keep old version; new deployments get new version.
func TestSEC04_SecretRotation_NewVersionWithoutBreakingRunningInstances(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockFactory, log)
	keyProvider := secrets.NewSingleKeyProvider([]byte("32-byte-master-encryption-key-01"))
	secretStore := secrets.NewMemorySecretStore(keyProvider)
	redactor := secrets.NewRedactor()
	depService.SetSecretStore(secretStore)
	depService.SetRedactor(redactor)

	_, _ = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "rotate-worker-1",
		Hostname:  "rotate-node-1",
		Capacity:  10,
	})

	projectID := "proj-rotate-test"
	secretName := "PAYMENT_API_KEY"
	valV1 := "v1-secret-key-1111"
	valV2 := "v2-rotated-key-2222"

	// 1. Set secret at Version 1
	secV1, err := secretStore.SetSecret(ctx, projectID, secretName, valV1)
	if err != nil {
		t.Fatalf("set secret v1: %v", err)
	}
	if secV1.Version != 1 {
		t.Fatalf("expected secret version 1, got %d", secV1.Version)
	}

	// 2. Launch Deployment 1 (in-flight workload)
	dep1, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "payment-service:v1",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("deploy 1: %v", err)
	}

	// Verify Deployment 1 container received v1
	if len(mockFactory.Dispatched) != 1 {
		t.Fatalf("expected 1 dispatched container, got %d", len(mockFactory.Dispatched))
	}
	ctr1 := mockFactory.Dispatched[0]
	if ctr1.Env[secretName] != valV1 {
		t.Fatalf("expected container 1 to have v1 secret %q, got %q", valV1, ctr1.Env[secretName])
	}

	// 3. Rotate Secret to Version 2
	secV2, err := secrets.RotateSecret(ctx, secretStore, projectID, secretName, valV2)
	if err != nil {
		t.Fatalf("rotate secret to v2: %v", err)
	}
	if secV2.Version != 2 {
		t.Fatalf("expected secret version 2 after rotation, got %d", secV2.Version)
	}

	// 4. Verify in-flight Deployment 1 snapshot and container are UNBROKEN
	dep1Secrets, err := secretStore.GetSecretsForDeployment(ctx, dep1.ID)
	if err != nil {
		t.Fatalf("get dep1 secrets: %v", err)
	}
	if dep1Secrets[secretName] != valV1 {
		t.Fatalf("SEC-04 violation: in-flight deployment lost old secret version! (got %q, want %q)",
			dep1Secrets[secretName], valV1)
	}
	// Verify in-flight container state remains running with old version
	if ctr1.Env[secretName] != valV1 {
		t.Fatalf("SEC-04 violation: in-flight container environment mutated unexpectedly!")
	}

	// 5. Launch Deployment 2 after rotation
	dep2, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "payment-service:v2",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("deploy 2: %v", err)
	}

	if len(mockFactory.Dispatched) != 2 {
		t.Fatalf("expected 2 dispatched containers, got %d", len(mockFactory.Dispatched))
	}
	ctr2 := mockFactory.Dispatched[1]
	if ctr2.Env[secretName] != valV2 {
		t.Fatalf("expected container 2 to have v2 secret %q, got %q", valV2, ctr2.Env[secretName])
	}

	dep2Secrets, err := secretStore.GetSecretsForDeployment(ctx, dep2.ID)
	if err != nil {
		t.Fatalf("get dep2 secrets: %v", err)
	}
	if dep2Secrets[secretName] != valV2 {
		t.Fatalf("expected dep2 secrets to have %q, got %q", valV2, dep2Secrets[secretName])
	}

	t.Log("SEC-04 Passed: Secret rotation creates a new version without breaking running instances (in-flight kept v1, new got v2)!")
}
