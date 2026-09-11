package security

import (
	"context"
	"strings"
	"testing"

	"github.com/nebula/nebula/internal/grpcapi"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSecurity_WorkerGRPC_HostilePayloadValidation tests that the Worker Agent strictly validates
// command payloads and rejects malformed, out-of-range, or injection-style input with InvalidArgument
// without forwarding any calls to the container runtime engine (§21.2).
func TestSecurity_WorkerGRPC_HostilePayloadValidation(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	setupServer := func() (*grpcapi.WorkerServiceServer, *runtime.MockDockerClient) {
		mockDocker := runtime.NewMockDockerClient()
		tracker := runtime.NewInstanceTracker()
		ops := runtime.NewContainerOps(mockDocker, tracker, log)
		server := grpcapi.NewWorkerServiceServer(ops, log)
		return server, mockDocker
	}

	t.Run("OutOfRangePort_Rejected", func(t *testing.T) {
		server, mockDocker := setupServer()

		// Test port 70000 (> 65535)
		req := &proto.RunContainerRequest{
			InstanceId:   "inst-port-overflow",
			DeploymentId: "dep-01",
			Image:        "alpine:latest",
			Ports: []*proto.PortMapping{
				{HostPort: 70000, ContainerPort: 8080, Protocol: "tcp"},
			},
		}

		resp, err := server.RunContainer(ctx, req)
		if err == nil {
			t.Fatalf("expected error for port 70000, got nil response: %+v", resp)
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument for port 70000, got code %v (err: %v)", status.Code(err), err)
		}

		// Test port 0 (<= 0)
		reqZero := &proto.RunContainerRequest{
			InstanceId:   "inst-port-zero",
			DeploymentId: "dep-01",
			Image:        "alpine:latest",
			Ports: []*proto.PortMapping{
				{HostPort: 80, ContainerPort: 0, Protocol: "tcp"},
			},
		}
		respZero, errZero := server.RunContainer(ctx, reqZero)
		if errZero == nil {
			t.Fatalf("expected error for port 0, got nil response: %+v", respZero)
		}
		if status.Code(errZero) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument for port 0, got code %v", status.Code(errZero))
		}

		// Ensure Docker was never touched
		if mockDocker.CreateCalls != 0 {
			t.Errorf("expected 0 Docker create calls, got %d", mockDocker.CreateCalls)
		}
	})

	t.Run("PathTraversalInImage_Rejected", func(t *testing.T) {
		server, mockDocker := setupServer()

		traversalImages := []string{
			"../../malicious/image:latest",
			"../shadow:v1",
			"..\\windows\\system32",
			"/etc/passwd:latest",
		}

		for _, img := range traversalImages {
			req := &proto.RunContainerRequest{
				InstanceId:   "inst-img-traversal",
				DeploymentId: "dep-01",
				Image:        img,
			}
			_, err := server.RunContainer(ctx, req)
			if err == nil {
				t.Fatalf("expected error for image with path traversal %q, got nil", img)
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("expected InvalidArgument for %q, got code %v", img, status.Code(err))
			}
		}

		if mockDocker.CreateCalls != 0 {
			t.Errorf("expected 0 Docker create calls, got %d", mockDocker.CreateCalls)
		}
	})

	t.Run("OversizedEnvValue_Rejected", func(t *testing.T) {
		server, mockDocker := setupServer()

		// 64KB env value (> 32KB limit)
		hugeVal := strings.Repeat("X", 64*1024)
		req := &proto.RunContainerRequest{
			InstanceId:   "inst-env-huge",
			DeploymentId: "dep-01",
			Image:        "alpine:latest",
			Env: map[string]string{
				"HUGE_PAYLOAD": hugeVal,
			},
		}

		_, err := server.RunContainer(ctx, req)
		if err == nil {
			t.Fatalf("expected error for oversized env value, got nil")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument for oversized env, got code %v", status.Code(err))
		}

		if mockDocker.CreateCalls != 0 {
			t.Errorf("expected 0 Docker create calls, got %d", mockDocker.CreateCalls)
		}
	})

	t.Run("ShellInjectionInEnv_Rejected", func(t *testing.T) {
		server, mockDocker := setupServer()

		injectionPayloads := []string{
			"$(rm -rf /)",
			"`cat /etc/shadow`",
			"test; rm -rf /",
		}

		for _, payload := range injectionPayloads {
			if strings.Contains(payload, "$(") || strings.Contains(payload, "`") {
				req := &proto.RunContainerRequest{
					InstanceId:   "inst-env-injection",
					DeploymentId: "dep-01",
					Image:        "alpine:latest",
					Env: map[string]string{
						"DATABASE_NAME": payload,
					},
				}
				_, err := server.RunContainer(ctx, req)
				if err == nil {
					t.Fatalf("expected error for shell injection payload %q, got nil", payload)
				}
				if status.Code(err) != codes.InvalidArgument {
					t.Errorf("expected InvalidArgument for payload %q, got code %v", payload, status.Code(err))
				}
			}
		}

		if mockDocker.CreateCalls != 0 {
			t.Errorf("expected 0 Docker create calls, got %d", mockDocker.CreateCalls)
		}
	})

	t.Run("ShellInjectionInLabels_Rejected", func(t *testing.T) {
		server, mockDocker := setupServer()

		req := &proto.RunContainerRequest{
			InstanceId:   "inst-label-injection",
			DeploymentId: "dep-01",
			Image:        "alpine:latest",
			Labels: map[string]string{
				"nebula.cmd": "echo $(whoami)",
			},
		}

		_, err := server.RunContainer(ctx, req)
		if err == nil {
			t.Fatalf("expected error for command substitution in label, got nil")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument for command substitution in label, got code %v", status.Code(err))
		}

		if mockDocker.CreateCalls != 0 {
			t.Errorf("expected 0 Docker create calls, got %d", mockDocker.CreateCalls)
		}
	})
}
