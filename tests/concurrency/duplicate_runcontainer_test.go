package concurrency

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/grpcapi"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestG09_OneInstanceIdentityMaxOneContainer validates G-09:
// When multiple concurrent RunContainer requests are dispatched with the exact same
// instance ID, exactly one container is created and all callers receive the same container ID.
func TestG09_OneInstanceIdentityMaxOneContainer(t *testing.T) {
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

	// Connect gRPC client
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := proto.NewWorkerServiceClient(conn)

	const concurrencyCount = 20
	const instanceID = "inst-concurrency-g09"
	const testImage = "alpine:3.19"

	var wg sync.WaitGroup
	var nonDuplicateCount int32
	var duplicateCount int32
	containerIDs := make([]string, concurrencyCount)
	errorsList := make([]error, concurrencyCount)

	startGate := make(chan struct{})

	for i := 0; i < concurrencyCount; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			<-startGate // Synchronized blast

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp, err := client.RunContainer(ctx, &proto.RunContainerRequest{
				InstanceId:   instanceID,
				DeploymentId: "dep-001",
				Image:        testImage,
				Env:          map[string]string{"PORT": "8080"},
				Ports: []*proto.PortMapping{
					{HostPort: 8080, ContainerPort: 80, Protocol: "tcp"},
				},
			})

			if err != nil {
				errorsList[idx] = err
				return
			}

			if resp.Error != "" {
				t.Errorf("unexpected error in response: %s", resp.Error)
				return
			}

			containerIDs[idx] = resp.ContainerId

			if resp.IsDuplicate {
				atomic.AddInt32(&duplicateCount, 1)
			} else {
				atomic.AddInt32(&nonDuplicateCount, 1)
			}
		}()
	}

	// Trigger all goroutines at the exact same instant
	close(startGate)
	wg.Wait()

	// Verification 1: No gRPC call errors
	for i, err := range errorsList {
		if err != nil {
			t.Fatalf("goroutine %d failed with error: %v", i, err)
		}
	}

	// Verification 2: Underlying Docker client called CreateContainer exactly ONCE
	if mockDocker.CreateCalls != 1 {
		t.Fatalf("expected exactly 1 Docker CreateContainer call, got %d", mockDocker.CreateCalls)
	}
	if mockDocker.StartCalls != 1 {
		t.Fatalf("expected exactly 1 Docker StartContainer call, got %d", mockDocker.StartCalls)
	}

	// Verification 3: Exactly one caller received is_duplicate = false, all others received is_duplicate = true
	if nonDuplicateCount != 1 {
		t.Fatalf("expected exactly 1 non-duplicate response, got %d", nonDuplicateCount)
	}
	if duplicateCount != concurrencyCount-1 {
		t.Fatalf("expected %d duplicate responses, got %d", concurrencyCount-1, duplicateCount)
	}

	// Verification 4: All callers received the identical container ID
	expectedID := containerIDs[0]
	if expectedID == "" {
		t.Fatalf("expected non-empty container ID")
	}
	for i, id := range containerIDs {
		if id != expectedID {
			t.Errorf("goroutine %d got container ID %s, expected %s", i, id, expectedID)
		}
	}

	// Verification 5: ListContainers confirms exactly 1 container exists
	listResp, err := client.ListContainers(context.Background(), &proto.ListContainersRequest{})
	if err != nil {
		t.Fatalf("failed to list containers: %v", err)
	}
	if len(listResp.Containers) != 1 {
		t.Fatalf("expected exactly 1 tracked container, got %d", len(listResp.Containers))
	}
	if listResp.Containers[0].ContainerId != expectedID {
		t.Fatalf("expected container ID %s in list, got %s", expectedID, listResp.Containers[0].ContainerId)
	}
}

// RACE-03 (Gate G-22): Duplicate/retried RunContainer at gRPC layer.
// Fire the same RunContainer request 10x in parallel; all converge to exactly one container.
func TestRACE03_G22_10xParallelRunContainerIdempotent(t *testing.T) {
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

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := proto.NewWorkerServiceClient(conn)

	const concurrencyCount = 10
	const instanceID = "inst-race03-parallel-10x"
	const testImage = "redis:7-alpine"

	var wg sync.WaitGroup
	var nonDuplicateCount int32
	var duplicateCount int32
	containerIDs := make([]string, concurrencyCount)
	startGate := make(chan struct{})

	for i := 0; i < concurrencyCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startGate // Synchronize all goroutines to fire at the exact same instant

			resp, callErr := client.RunContainer(context.Background(), &proto.RunContainerRequest{
				InstanceId:   instanceID,
				DeploymentId: "dep-race03",
				Image:        testImage,
			})
			if callErr != nil || resp.Error != "" {
				t.Errorf("call %d error: %v, resp error: %s", idx, callErr, resp.GetError())
				return
			}

			containerIDs[idx] = resp.ContainerId
			if resp.IsDuplicate {
				atomic.AddInt32(&duplicateCount, 1)
			} else {
				atomic.AddInt32(&nonDuplicateCount, 1)
			}
		}(i)
	}

	close(startGate)
	wg.Wait()

	if nonDuplicateCount != 1 {
		t.Fatalf("RACE-03 VIOLATION: expected exactly 1 non-duplicate creation, got %d", nonDuplicateCount)
	}
	if duplicateCount != concurrencyCount-1 {
		t.Fatalf("RACE-03 VIOLATION: expected %d duplicate responses, got %d", concurrencyCount-1, duplicateCount)
	}

	// Confirm all 10 parallel callers got identical container ID
	firstID := containerIDs[0]
	for i, id := range containerIDs {
		if id != firstID {
			t.Fatalf("RACE-03 VIOLATION: caller %d got different container ID %s != %s", i, id, firstID)
		}
	}

	t.Logf("RACE-03 (Gate G-22) Passed: %d parallel RunContainer requests converged to exactly 1 container (%s)!",
		concurrencyCount, firstID)
}
