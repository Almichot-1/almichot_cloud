package security

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/grpcapi"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// §14 / §21.3 / Phase 12 Security Re-verification:
// Build Worker execution on a dedicated fleet must never receive Control Plane secrets.
func TestSecurity_BuildWorkerFleet_NoCPSecretsLeaked(t *testing.T) {
	log := zerolog.Nop()
	node := build.NewBuildWorkerNode(build.BuildWorkerNodeConfig{
		WorkerKey: "build-fleet-sec-01",
		Hostname:  "bld-sec-01.internal",
		IPAddress: "10.0.10.5",
		Port:      9091,
		Capacity:  2,
	}, log)

	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"echo\", \"secure\"]"), 0644)

	// Sensitive Control Plane secrets and internal routing
	sensitiveSecrets := map[string]string{
		"NEBULA_DB_PASSWORD":   "ultra-secret-db-pass-1234",
		"NEBULA_SECRETS_KEY":   "32byte-aes-gcm-master-envelope-key",
		"DATABASE_URL":         "postgres://admin:pass@db.internal:5432/nebula",
		"POSTGRES_PASSWORD":    "postgres-root-secret",
		"API_ADMIN_TOKEN":      "jwt-super-admin-token",
		"CONTROL_PLANE_ROUTE":  "http://cp.internal:9090",
		"INTERNAL_GRPC_ADDR":   "127.0.0.1:9090",
		"PUBLIC_SAFE_CONFIG":   "production-mode",
	}

	plan := &build.BuildPlan{
		Strategy:          build.StrategyDockerfile,
		DockerfileContent: "FROM alpine:latest\nCMD [\"echo\", \"secure\"]",
	}

	res, err := node.ExecuteBuild(context.Background(), build.BuildRequest{
		ProjectID: "proj-sec-fleet",
		SourceDir: tmpDir,
		Plan:      plan,
		ImageTag:  "nebula/sec-fleet:v1",
		BuildArgs: sensitiveSecrets,
	})
	if err != nil {
		t.Fatalf("build on dedicated worker failed: %v", err)
	}

	// 1. Assert no CP secret values appear in the build log
	for k, v := range sensitiveSecrets {
		if k == "PUBLIC_SAFE_CONFIG" {
			continue
		}
		if strings.Contains(res.BuildLog, v) {
			t.Fatalf("SECURITY VIOLATION (§14/§21.3): secret %s leaked into build log!", k)
		}
		if strings.Contains(string(res.ImageData), v) {
			t.Fatalf("SECURITY VIOLATION (§14/§21.3): secret %s leaked into image artifact payload!", k)
		}
	}

	t.Log("PASSED: Dedicated Build Worker sandbox executed with zero CP secrets leaked (§14/§21.3)")
}

// §14 / §21.3 / Phase 12 Security Re-verification:
// A Build Worker's identity cannot be used to invoke runtime container control paths
// (RunContainer, StopContainer, ListContainers).
func TestSecurity_BuildWorkerFleet_CannotReachGRPCControlPath(t *testing.T) {
	log := zerolog.Nop()
	mockClient := runtime.NewMockDockerClient()
	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(mockClient, tracker, log)
	srv := grpcapi.NewWorkerServiceServer(ops, log)

	// Context with build worker identity credentials (role=build, capability=build)
	buildCtx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("role", "build", "capability", "build"),
	)

	// 1. RunContainer from build worker identity must fail with PermissionDenied
	_, err := srv.RunContainer(buildCtx, &proto.RunContainerRequest{
		InstanceId: "inst-malicious-01",
		Image:      "malicious/image:latest",
	})
	if err == nil {
		t.Fatalf("expected RunContainer to fail when called with build worker credentials")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected codes.PermissionDenied, got: %v (code=%v)", err, st.Code())
	}

	// 2. StopContainer from build worker identity must fail with PermissionDenied
	_, err = srv.StopContainer(buildCtx, &proto.StopContainerRequest{
		InstanceId:     "inst-malicious-01",
		TimeoutSeconds: 5,
	})
	if err == nil {
		t.Fatalf("expected StopContainer to fail when called with build worker credentials")
	}
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected codes.PermissionDenied, got: %v (code=%v)", err, st.Code())
	}

	// 3. ListContainers from build worker identity must fail with PermissionDenied
	_, err = srv.ListContainers(buildCtx, &proto.ListContainersRequest{})
	if err == nil {
		t.Fatalf("expected ListContainers to fail when called with build worker credentials")
	}
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected codes.PermissionDenied, got: %v (code=%v)", err, st.Code())
	}

	// 4. In contrast, runtime worker identity without build role succeeds
	runtimeCtx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("role", "control-plane"),
	)
	listRes, err := srv.ListContainers(runtimeCtx, &proto.ListContainersRequest{})
	if err != nil {
		t.Fatalf("expected authorized runtime call to succeed, got: %v", err)
	}
	if listRes == nil {
		t.Fatalf("expected non-nil response for authorized call")
	}

	t.Log("PASSED: Dedicated Build Worker identity is strictly forbidden from gRPC control path (§14/§21.3)")
}
