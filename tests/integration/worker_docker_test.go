package integration

import (
	"context"
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

// TestWorkerAgent_Phase1_ExitCheck validates the complete Phase 1 functionality:
// 1. Worker registration RPC (Register)
// 2. Hand-written gRPC client calling RunContainer twice with the same instance ID and getting one container (Exit Check)
// 3. Container status inspection (GetContainerStatus)
// 4. Container listing (ListContainers)
// 5. Container stopping (StopContainer)
func TestWorkerAgent_Phase1_ExitCheck(t *testing.T) {
	log := zerolog.Nop()
	mockDocker := runtime.NewMockDockerClient()
	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(mockDocker, tracker, log)
	workerSvc := grpcapi.NewWorkerServiceServer(ops, log)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on ephemeral port: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	proto.RegisterWorkerServiceServer(grpcServer, workerSvc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.GracefulStop()

	// Hand-written gRPC client
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to connect to worker gRPC server: %v", err)
	}
	defer conn.Close()

	client := proto.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Step 1: Worker registration RPC (Register)
	regResp, err := client.Register(ctx, &proto.RegisterRequest{
		WorkerId:  "worker-node-alpha",
		Hostname:  "nebula-worker-01",
		IpAddress: "192.168.1.100",
		Capacity:  8,
		Labels: map[string]string{
			"region": "us-east-1",
			"tier":   "standard",
		},
	})
	if err != nil {
		t.Fatalf("Register RPC failed: %v", err)
	}
	if !regResp.Success {
		t.Fatalf("Register RPC returned success=false, message: %s", regResp.Message)
	}
	t.Logf("Worker registration succeeded: %s", regResp.Message)

	// Step 2: First RunContainer call
	const instanceID = "inst-phase1-exit-check"
	const image = "redis:alpine"

	runResp1, err := client.RunContainer(ctx, &proto.RunContainerRequest{
		InstanceId:   instanceID,
		DeploymentId: "deploy-xyz",
		Image:        image,
		Env: map[string]string{
			"REDIS_PORT": "6379",
		},
		Ports: []*proto.PortMapping{
			{HostPort: 6379, ContainerPort: 6379, Protocol: "tcp"},
		},
		Labels: map[string]string{
			"app": "cache",
		},
	})
	if err != nil {
		t.Fatalf("first RunContainer failed: %v", err)
	}
	if runResp1.Error != "" {
		t.Fatalf("first RunContainer returned error: %s", runResp1.Error)
	}
	if runResp1.ContainerId == "" {
		t.Fatalf("first RunContainer returned empty container ID")
	}
	if runResp1.IsDuplicate {
		t.Fatalf("expected first call to NOT be duplicate, but is_duplicate=true")
	}
	firstContainerID := runResp1.ContainerId
	t.Logf("First RunContainer created container: %s", firstContainerID)

	// Step 3: Second RunContainer call with the SAME instance ID (EXIT CHECK)
	runResp2, err := client.RunContainer(ctx, &proto.RunContainerRequest{
		InstanceId:   instanceID,
		DeploymentId: "deploy-xyz",
		Image:        image,
	})
	if err != nil {
		t.Fatalf("second RunContainer failed: %v", err)
	}
	if runResp2.Error != "" {
		t.Fatalf("second RunContainer returned error: %s", runResp2.Error)
	}
	if !runResp2.IsDuplicate {
		t.Fatalf("expected second RunContainer to be marked duplicate (is_duplicate=true)")
	}
	if runResp2.ContainerId != firstContainerID {
		t.Fatalf("expected same container ID %s, got %s", firstContainerID, runResp2.ContainerId)
	}
	t.Logf("Second RunContainer returned existing container (Exit Check passed): %s", runResp2.ContainerId)

	// Verify Docker client was only invoked once to create and once to start
	if mockDocker.CreateCalls != 1 {
		t.Fatalf("expected exactly 1 Docker Create call, got %d", mockDocker.CreateCalls)
	}
	if mockDocker.StartCalls != 1 {
		t.Fatalf("expected exactly 1 Docker Start call, got %d", mockDocker.StartCalls)
	}

	// Step 4: GetContainerStatus
	statusResp, err := client.GetContainerStatus(ctx, &proto.GetContainerStatusRequest{
		InstanceId: instanceID,
	})
	if err != nil {
		t.Fatalf("GetContainerStatus failed: %v", err)
	}
	if statusResp.Status != "running" {
		t.Fatalf("expected container status 'running', got '%s'", statusResp.Status)
	}
	if statusResp.ContainerId != firstContainerID {
		t.Fatalf("expected container ID %s, got %s", firstContainerID, statusResp.ContainerId)
	}

	// Step 5: ListContainers
	listResp, err := client.ListContainers(ctx, &proto.ListContainersRequest{})
	if err != nil {
		t.Fatalf("ListContainers failed: %v", err)
	}
	if len(listResp.Containers) != 1 {
		t.Fatalf("expected exactly 1 container in list, got %d", len(listResp.Containers))
	}
	if listResp.Containers[0].InstanceId != instanceID {
		t.Fatalf("expected instance ID %s in list, got %s", instanceID, listResp.Containers[0].InstanceId)
	}

	// Step 6: StopContainer
	stopResp, err := client.StopContainer(ctx, &proto.StopContainerRequest{
		InstanceId:     instanceID,
		TimeoutSeconds: 5,
	})
	if err != nil {
		t.Fatalf("StopContainer failed: %v", err)
	}
	if !stopResp.Success {
		t.Fatalf("expected StopContainer success=true, got error: %s", stopResp.Error)
	}

	// Step 7: Verify status after stop
	statusRespAfterStop, err := client.GetContainerStatus(ctx, &proto.GetContainerStatusRequest{
		InstanceId: instanceID,
	})
	if err != nil {
		t.Fatalf("GetContainerStatus after stop failed: %v", err)
	}
	if statusRespAfterStop.Status != "stopped" {
		t.Fatalf("expected status 'stopped', got '%s'", statusRespAfterStop.Status)
	}

	t.Log("Phase 1 Exit Check completely satisfied!")
}
