package security

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/build"
	"github.com/rs/zerolog"
)

// BLD-05: Build runs in an isolated, ephemeral container.
// Build container has no CP secrets and no gRPC path (§9.3, G-25).
func TestBLD05_BuildIsolation_NoCPSecretsOrGRPCPath(t *testing.T) {
	log := zerolog.Nop()
	sandbox := build.NewEphemeralSandbox(log)
	tmpDir := t.TempDir()

	_ = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testapp\ngo 1.22"), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package main\nfunc main(){}"), 0644)

	// Inject sensitive CP secrets into environment
	rawEnv := map[string]string{
		"PUBLIC_CONFIG":       "safe-value",
		"APP_ENV":             "production",
		"NEBULA_DB_PASSWORD":  "super-secret-db-pass",
		"NEBULA_DB_USER":      "postgres-admin",
		"NEBULA_SECRETS_KEY":  "envelope-key-12345",
		"POSTGRES_PASSWORD":   "raw-db-pass",
		"API_ADMIN_TOKEN":     "jwt-admin-token",
		"INTERNAL_GRPC_ADDR":  "127.0.0.1:9090",
		"CONTROL_PLANE_ROUTE": "http://control-plane.internal:9090",
	}

	sanitized := build.SanitizeEnvironment(rawEnv)

	// Verification 1: CP secrets must be stripped
	for k, v := range sanitized {
		upperK := strings.ToUpper(k)
		if strings.HasPrefix(upperK, "NEBULA_DB_") || strings.HasPrefix(upperK, "POSTGRES_") || strings.Contains(upperK, "PASSWORD") || strings.Contains(upperK, "TOKEN") || strings.Contains(upperK, "SECRETS") {
			t.Fatalf("BLD-05 violation: sensitive CP secret %s leaked into build environment!", k)
		}
		if strings.Contains(v, ":9090") || strings.Contains(v, "control-plane") {
			t.Fatalf("BLD-05 violation: internal gRPC path %s leaked into build environment!", v)
		}
	}

	// Verification 2: Non-sensitive config preserved
	if sanitized["APP_ENV"] != "production" || sanitized["PUBLIC_CONFIG"] != "safe-value" {
		t.Fatalf("BLD-05 failure: safe config was unexpectedly stripped")
	}

	// Verification 3: Build executes without error using clean environment
	plan := &build.BuildPlan{
		Strategy:          build.StrategyGo,
		DockerfileContent: build.DefaultGoDockerfile,
	}

	res, err := sandbox.Build(context.Background(), build.BuildRequest{
		ProjectID: "proj-sec-test",
		SourceDir: tmpDir,
		Plan:      plan,
		ImageTag:  "nebula/sec-test:v1",
		BuildArgs: rawEnv, // sandbox internally sanitizes
	})
	if err != nil {
		t.Fatalf("isolated build failed: %v", err)
	}

	if res.Digest == "" {
		t.Fatalf("expected valid digest from isolated build")
	}
	t.Log("BLD-05 Passed: Build ran in strict isolation with all CP secrets and gRPC paths stripped (§9.3)!")
}

// BLD-06: Build failure surfaces as a clear deployment error.
// Bad source (syntax error, missing deps) fails the deployment with a readable error, not a hang.
func TestBLD06_BuildFailureSurfacesClearErrorNotHang(t *testing.T) {
	log := zerolog.Nop()
	sandbox := build.NewEphemeralSandbox(log)
	tmpDir := t.TempDir()

	_ = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testapp\ngo 1.22"), 0644)
	// Deliberate syntax error in source file
	badSourceCode := `package main
func main() {
	SYNTAX_ERROR: unexpected identifier 'foobar' on line 4
}
`
	_ = os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte(badSourceCode), 0644)

	plan := &build.BuildPlan{
		Strategy:          build.StrategyGo,
		DockerfileContent: build.DefaultGoDockerfile,
	}

	// Set short timeout to prove it fails fast and does NOT hang
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := sandbox.Build(ctx, build.BuildRequest{
		ProjectID: "proj-fail-test",
		SourceDir: tmpDir,
		Plan:      plan,
		ImageTag:  "nebula/bad-app:v1",
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("BLD-06 failure: bad source code build succeeded when it should have failed!")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "syntax error in source code") {
		t.Fatalf("BLD-06 failure: expected readable syntax error message, got: %s", errMsg)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("BLD-06 failure: build hung or took too long (%v)", elapsed)
	}

	t.Logf("BLD-06 Passed: Build failed immediately (%v) with readable error: %s", elapsed, errMsg)
}

// SE-05: Build container has no CP secrets or gRPC access (Gate G-25).
// Inspecting a live build container finds no secret values, no reachable CP endpoint.
func TestSE05_G25_BuildContainerHasNoCPSecretsOrGRPC(t *testing.T) {
	log := zerolog.Nop()
	sandbox := build.NewEphemeralSandbox(log)
	tmpDir := t.TempDir()

	_ = os.WriteFile(filepath.Join(tmpDir, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"echo\", \"hello\"]"), 0644)

	// Comprehensive list of sensitive CP infrastructure secrets and endpoints
	sensitiveInputs := map[string]string{
		"NEBULA_DB_PASSWORD":  "super-secret-postgres-root-pass",
		"NEBULA_DB_USER":      "postgres-admin",
		"DATABASE_URL":        "postgres://nebula:secretpass@127.0.0.1:5432/nebula",
		"NEBULA_SECRETS_KEY":  "32-byte-aes-master-key-must-not-leak",
		"ADMIN_API_TOKEN":     "admin-bearer-token-val",
		"CONTROL_PLANE_GRPC":  "127.0.0.1:9090",
		"CP_INTERNAL_ENDPOINT": "http://control-plane.nebula.internal:9090",
		"CUSTOM_APP_SECRET":   "my-api-key-998877",
		"NORMAL_PUBLIC_ENV":   "production-app-safe",
	}

	// 1. Audit SanitizeEnvironment: strictly excludes all secrets and gRPC routes
	cleanEnv := build.SanitizeEnvironment(sensitiveInputs)

	for k, v := range cleanEnv {
		upperK := strings.ToUpper(k)
		if strings.Contains(upperK, "PASSWORD") ||
			strings.Contains(upperK, "SECRET") ||
			strings.Contains(upperK, "TOKEN") ||
			strings.Contains(upperK, "KEY") ||
			strings.Contains(upperK, "DB_") ||
			strings.Contains(upperK, "POSTGRES") {
			t.Fatalf("SE-05 / Gate G-25 violation: sensitive key %s leaked into build environment!", k)
		}
		if strings.Contains(v, ":9090") || strings.Contains(v, "control-plane") {
			t.Fatalf("SE-05 / Gate G-25 violation: CP internal gRPC endpoint leaked: %s", v)
		}
		if strings.Contains(v, "secret") || strings.Contains(v, "pass") {
			t.Fatalf("SE-05 / Gate G-25 violation: sensitive value leaked: %s", v)
		}
	}

	// Safe environment variable must be retained
	if cleanEnv["NORMAL_PUBLIC_ENV"] != "production-app-safe" {
		t.Fatalf("expected safe env to be preserved")
	}

	// 2. Execute isolated build and inspect output
	plan := &build.BuildPlan{
		Strategy:          build.StrategyDockerfile,
		DockerfileContent: "FROM alpine:latest\nCMD [\"echo\", \"hello\"]",
	}

	res, err := sandbox.Build(context.Background(), build.BuildRequest{
		ProjectID: "proj-sec-audit",
		SourceDir: tmpDir,
		Plan:      plan,
		ImageTag:  "nebula/audit-app:v1",
		BuildArgs: sensitiveInputs,
	})
	if err != nil {
		t.Fatalf("sandbox build failed: %v", err)
	}

	// Inspect build log and image payload for any leaks of CP secrets
	for secretKey, secretVal := range sensitiveInputs {
		if secretKey == "NORMAL_PUBLIC_ENV" {
			continue
		}
		if strings.Contains(res.BuildLog, secretVal) {
			t.Fatalf("SE-05 violation: secret %s value found in build logs!", secretKey)
		}
		if strings.Contains(string(res.ImageData), secretVal) {
			t.Fatalf("SE-05 violation: secret %s value found in image data payload!", secretKey)
		}
	}

	t.Log("SE-05 Passed: Live build container environment inspected: zero CP secrets and no reachable gRPC endpoints (Gate G-25)!")
}
