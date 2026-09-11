package integration

import (
	"context"
	"fmt"
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

// TestDocker_ResourceLimitsEnforcedAtCgroupLevel verifies that cpu_limit and memory_limit_mb
// specified in RunContainerRequest are translated to Docker cgroup HostConfig.Resources (§12.1, §14.2)
// and actually applied by the Docker engine when starting a container.
func TestDocker_ResourceLimitsEnforcedAtCgroupLevel(t *testing.T) {
	log := zerolog.Nop()
	realClient, err := runtime.NewRealDockerClient(log)
	if err != nil {
		t.Skipf("real docker client unavailable, skipping: %v", err)
	}
	defer realClient.Close()

	probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = realClient.ListContainers(probeCtx)
	probeCancel()
	if err != nil {
		t.Skipf("docker daemon not reachable, skipping: %v", err)
	}

	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(realClient, tracker, log)
	workerSvc := grpcapi.NewWorkerServiceServer(ops, log)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	proto.RegisterWorkerServiceServer(grpcServer, workerSvc)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.GracefulStop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewWorkerServiceClient(conn)
	rootCtx, rootCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer rootCancel()

	instanceID := fmt.Sprintf("inst-limits-%d", time.Now().UnixNano())
	const image = "alpine:3.20"
	const expectedCPUs = 1.0
	const expectedMemoryMB = 512
	const expectedNanoCPUs int64 = 1_000_000_000
	const expectedMemoryBytes int64 = 512 * 1024 * 1024

	var containerID string
	t.Cleanup(func() {
		if containerID != "" {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = realClient.RemoveContainer(cleanupCtx, containerID, true)
		}
	})

	runRes, err := client.RunContainer(rootCtx, &proto.RunContainerRequest{
		InstanceId:    instanceID,
		DeploymentId:  "deploy-cgroup-limits",
		Image:         image,
		CpuLimit:      expectedCPUs,
		MemoryLimitMb: expectedMemoryMB,
		Labels: map[string]string{
			"nebula.cmd": "sleep 60",
		},
	})
	if err != nil {
		t.Fatalf("RunContainer with resource limits failed: %v", err)
	}
	if runRes.Error != "" || runRes.ContainerId == "" {
		t.Fatalf("RunContainer failed to start container: %+v", runRes)
	}
	containerID = runRes.ContainerId

	// 1. Verify via Docker's native Inspect API (Engine cgroup level)
	inspect, err := realClient.RawClient().ContainerInspect(rootCtx, containerID)
	if err != nil {
		t.Fatalf("Docker ContainerInspect failed: %v", err)
	}

	if inspect.HostConfig == nil {
		t.Fatalf("expected container HostConfig to be populated")
	}

	if inspect.HostConfig.Resources.NanoCPUs != expectedNanoCPUs {
		t.Errorf("cgroup NanoCPUs mismatch: expected %d (1 CPU), got %d", expectedNanoCPUs, inspect.HostConfig.Resources.NanoCPUs)
	}
	if inspect.HostConfig.Resources.Memory != expectedMemoryBytes {
		t.Errorf("cgroup Memory limit mismatch: expected %d bytes (512MB), got %d bytes", expectedMemoryBytes, inspect.HostConfig.Resources.Memory)
	}

	// 2. Verify via worker runtime's InspectContainer abstraction
	details, err := realClient.InspectContainer(rootCtx, containerID)
	if err != nil {
		t.Fatalf("InspectContainer failed: %v", err)
	}
	if details.NanoCPUs != expectedNanoCPUs {
		t.Errorf("runtime ContainerDetails NanoCPUs mismatch: expected %d, got %d", expectedNanoCPUs, details.NanoCPUs)
	}
	if details.Memory != expectedMemoryBytes {
		t.Errorf("runtime ContainerDetails Memory mismatch: expected %d, got %d", expectedMemoryBytes, details.Memory)
	}
}
