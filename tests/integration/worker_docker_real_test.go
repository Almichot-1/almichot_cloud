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

// TestWA08_RealDockerDaemon exercises the full worker flow against a real Docker
// daemon. It skips if a daemon is not reachable (e.g. Docker Desktop not running).
func TestWA08_RealDockerDaemon(t *testing.T) {
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
	rootCtx, rootCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer rootCancel()

	if _, err := client.Register(rootCtx, &proto.RegisterRequest{
		WorkerId:  "worker-wa08",
		Hostname:  "h",
		IpAddress: "127.0.0.1",
		Capacity:  4,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	instanceID := fmt.Sprintf("inst-wa08-%d", time.Now().UnixNano())
	const image = "redis:alpine"

	var containerID string
	t.Cleanup(func() {
		if containerID == "" {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = realClient.RemoveContainer(cleanupCtx, containerID, true)
	})

	runRes, err := client.RunContainer(rootCtx, &proto.RunContainerRequest{
		InstanceId:   instanceID,
		DeploymentId: "deploy-wa08",
		Image:        image,
	})
	if err != nil {
		t.Fatalf("first RunContainer: %v", err)
	}
	if runRes.Error != "" || runRes.ContainerId == "" {
		t.Fatalf("first RunContainer bad result: %+v", runRes)
	}
	if runRes.IsDuplicate {
		t.Fatalf("expected first run to NOT be duplicate")
	}
	containerID = runRes.ContainerId
	t.Logf("created real container %s", containerID)

	runRes2, err := client.RunContainer(rootCtx, &proto.RunContainerRequest{
		InstanceId:   instanceID,
		DeploymentId: "deploy-wa08",
		Image:        image,
	})
	if err != nil {
		t.Fatalf("second RunContainer: %v", err)
	}
	if !runRes2.IsDuplicate {
		t.Fatalf("expected second run to be duplicate")
	}
	if runRes2.ContainerId != containerID {
		t.Fatalf("expected same container id %s, got %s", containerID, runRes2.ContainerId)
	}

	status, err := client.GetContainerStatus(rootCtx, &proto.GetContainerStatusRequest{InstanceId: instanceID})
	if err != nil {
		t.Fatalf("GetContainerStatus: %v", err)
	}
	if status.Status != "running" {
		t.Fatalf("expected status running, got %q", status.Status)
	}

	list, err := client.ListContainers(rootCtx, &proto.ListContainersRequest{})
	if err != nil {
		t.Fatalf("ListContainers: %v", err)
	}
	found := false
	for _, c := range list.Containers {
		if c.InstanceId == instanceID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("instance %s not found in ListContainers: %+v", instanceID, list.Containers)
	}

	stop, err := client.StopContainer(rootCtx, &proto.StopContainerRequest{InstanceId: instanceID, TimeoutSeconds: 5})
	if err != nil {
		t.Fatalf("StopContainer: %v", err)
	}
	if !stop.Success {
		t.Fatalf("StopContainer failed: %s", stop.Error)
	}

	statusAfter, err := client.GetContainerStatus(rootCtx, &proto.GetContainerStatusRequest{InstanceId: instanceID})
	if err != nil {
		t.Fatalf("GetContainerStatus after stop: %v", err)
	}
	if statusAfter.Status == "running" {
		t.Fatalf("expected container to not be running after stop, got %q", statusAfter.Status)
	}
}