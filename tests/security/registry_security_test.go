package security

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/registry"
	"github.com/rs/zerolog"
)

// TestRegistrySecurity_CrossTenantPushRejected verifies that the build sandbox
// cannot push image references outside of its own project namespace (§20).
func TestRegistrySecurity_CrossTenantPushRejected(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)

	srcDir := filepath.Join(t.TempDir(), "app-security-push")
	_ = os.MkdirAll(srcDir, 0755)
	_ = os.WriteFile(filepath.Join(srcDir, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"app\"]"), 0644)

	tenantA := "proj-tenant-alpha"
	tenantB := "proj-tenant-beta"

	// 1. Authorized push to own tenant namespace succeeds
	authorizedTag := "nebula/" + tenantA + ":v1.0"
	resAuth, errAuth := orchestrator.BuildFromSource(ctx, tenantA, srcDir, authorizedTag, nil)
	if errAuth != nil {
		t.Fatalf("expected authorized push to succeed: %v", errAuth)
	}
	if resAuth == nil || resAuth.ImageTag != authorizedTag {
		t.Fatalf("unexpected build result: %v", resAuth)
	}

	// 2. Cross-tenant push attempt (Tenant A trying to push to Tenant B's repository)
	crossTenantTag := "nebula/" + tenantB + ":malicious-tag"
	_, errCross := orchestrator.BuildFromSource(ctx, tenantA, srcDir, crossTenantTag, nil)
	if errCross == nil {
		t.Fatalf("expected cross-tenant push to be rejected, but it succeeded")
	}
	if !strings.Contains(strings.ToLower(errCross.Error()), "unauthorized") {
		t.Fatalf("expected unauthorized error, got: %v", errCross)
	}

	// 3. Attempting to push to official/core system repository
	officialTag := "nebula/system-core:latest"
	_, errOfficial := orchestrator.BuildFromSource(ctx, tenantA, srcDir, officialTag, nil)
	if errOfficial == nil {
		t.Fatalf("expected push to official namespace by tenant to be rejected")
	}

	// 4. Test AuthorizePush utility directly
	if err := registry.AuthorizePush(tenantA, "nebula/proj-victim:latest"); err == nil {
		t.Fatalf("AuthorizePush failed to reject unauthorized tag")
	}
	if err := registry.AuthorizePush(tenantA, "nebula/proj-tenant-alpha:v1"); err != nil {
		t.Fatalf("AuthorizePush erroneously rejected authorized tag: %v", err)
	}
}

// TestRegistrySecurity_SigningCredentialsNotExposedToBuild verifies that
// image signing private keys are never exposed or leaked to the build environment (§20, Phase 12).
func TestRegistrySecurity_SigningCredentialsNotExposedToBuild(t *testing.T) {
	privKey, _, err := registry.GenerateSigningKeyPair()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Signer with cluster private key
	signer := registry.NewEd25519ImageSigner(privKey)
	if signer.PublicKey() == nil {
		t.Fatalf("expected public key")
	}

	// Inject hypothetical signing key env vars into input environment
	rawEnv := map[string]string{
		"NEBULA_SIGNING_PRIVATE_KEY": "ed25519-cluster-root-secret-key",
		"SIGNING_KEY_PATH":           "/etc/nebula/signing.key",
		"CLUSTER_PRIVATE_KEY":        "secret-cluster-data",
		"APP_NAME":                   "my-web-app",
		"PORT":                       "8080",
	}

	// Sanitize build environment
	sanitized := build.SanitizeEnvironment(rawEnv)

	// Verify all private keys, signing credentials, and secrets were stripped
	if _, leaked := sanitized["NEBULA_SIGNING_PRIVATE_KEY"]; leaked {
		t.Fatalf("NEBULA_SIGNING_PRIVATE_KEY leaked to build sandbox!")
	}
	if _, leaked := sanitized["SIGNING_KEY_PATH"]; leaked {
		t.Fatalf("SIGNING_KEY_PATH leaked to build sandbox!")
	}
	if _, leaked := sanitized["CLUSTER_PRIVATE_KEY"]; leaked {
		t.Fatalf("CLUSTER_PRIVATE_KEY leaked to build sandbox!")
	}

	// Safe variables must remain intact
	if sanitized["APP_NAME"] != "my-web-app" || sanitized["PORT"] != "8080" {
		t.Fatalf("safe environment variables unexpectedly stripped")
	}
}
