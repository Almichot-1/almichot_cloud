package failure

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/grpcapi"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestG14_DockerFailureReporting validates the groundwork for G-14:
// When Docker operations fail (create or start), the failure is cleanly reported
// via structured response and logged, while the worker agent remains resilient and responsive.
func TestG14_DockerFailureReporting(t *testing.T) {
	log := zerolog.Nop()
	mockDocker := runtime.NewMockDockerClient()
	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(mockDocker, tracker, log)
	workerSvc := grpcapi.NewWorkerServiceServer(ops, log)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	proto.RegisterWorkerServiceServer(grpcServer, workerSvc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.GracefulStop()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Scenario 1: Docker create failure injection
	mockDocker.FailCreate = errors.New("docker daemon: no space left on device")
	resp, err := client.RunContainer(ctx, &proto.RunContainerRequest{
		InstanceId: "inst-fail-create-1",
		Image:      "nginx:latest",
	})
	if err != nil {
		t.Fatalf("gRPC call itself should succeed and return structured error: %v", err)
	}
	if resp.Status != "FAILED" {
		t.Fatalf("expected status FAILED, got %s", resp.Status)
	}
	if resp.Error == "" {
		t.Fatalf("expected error message in response, got empty")
	}
	t.Logf("Create failure cleanly reported: %s", resp.Error)

	// Scenario 2: Docker start failure injection
	mockDocker.FailCreate = nil
	mockDocker.FailStart = errors.New("container start error: OCI runtime create failed")
	resp2, err := client.RunContainer(ctx, &proto.RunContainerRequest{
		InstanceId: "inst-fail-start-1",
		Image:      "nginx:latest",
	})
	if err != nil {
		t.Fatalf("gRPC call itself should succeed and return structured error: %v", err)
	}
	if resp2.Status != "FAILED" {
		t.Fatalf("expected status FAILED, got %s", resp2.Status)
	}
	if resp2.Error == "" {
		t.Fatalf("expected error message in response, got empty")
	}
	t.Logf("Start failure cleanly reported: %s", resp2.Error)

	// Scenario 3: Worker is still alive and succeeds when Docker is healthy
	mockDocker.FailStart = nil
	resp3, err := client.RunContainer(ctx, &proto.RunContainerRequest{
		InstanceId: "inst-healthy-1",
		Image:      "nginx:latest",
	})
	if err != nil {
		t.Fatalf("healthy RunContainer failed: %v", err)
	}
	if resp3.Status != "RUNNING" {
		t.Fatalf("expected status RUNNING, got %s", resp3.Status)
	}
	t.Logf("Worker remained fully functional after failure recovery: %s", resp3.ContainerId)
}
