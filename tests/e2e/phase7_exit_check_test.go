package e2e

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/grpcapi"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Phase 7 Exit Check:
// Part 1: Fire two deploys for the same project concurrently.
// Part 2: Fire the same RunContainer request 10x in parallel.
// Both converge to one correct outcome with zero duplicate work or corrupted state.
func TestPhase7_ExitCheck_ConcurrentDeploysAnd10xRunContainer(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	t.Log("=========================================================================")
	t.Log("STARTING PHASE 7 EXIT CHECK: CONCURRENCY & IDEMPOTENCY HARDENING")
	t.Log("=========================================================================")

	// =========================================================================
	// PART 1: Fire two deploys for the same project concurrently
	// =========================================================================
	t.Log("=== Testing Exit Check Part 1: Two Concurrent Deploys to Same Project ===")

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockClientFactory := deployments.NewMockWorkerClientFactory()

	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-phase7-exit",
		Hostname:  "node-phase7",
		IPAddress: "10.20.1.1",
		Capacity:  20,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

	const projectID = "proj-e2e-phase7"
	var wgDeploy sync.WaitGroup
	deployErrors := make([]error, 2)
	deploys := make([]*deployments.Deployment, 2)
	deployGate := make(chan struct{})

	for i := 0; i < 2; i++ {
		wgDeploy.Add(1)
		go func(idx int) {
			defer wgDeploy.Done()
			<-deployGate // fire both deploys at the exact same instant

			imageTag := "web-service:v1"
			if idx == 1 {
				imageTag = "web-service:v2"
			}

			dep, _, err := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
				ProjectID:     projectID,
				Image:         imageTag,
				InstanceCount: 1,
			})
			if err != nil {
				deployErrors[idx] = err
				return
			}
			deploys[idx] = dep
		}(i)
	}

	close(deployGate)
	wgDeploy.Wait()

	for i, err := range deployErrors {
		if err != nil {
			t.Fatalf("Exit check part 1 failure: deploy %d failed: %v", i+1, err)
		}
	}

	// Verify both deployments completed to RUNNING without corrupted DB records
	if deploys[0] == nil || deploys[1] == nil {
		t.Fatalf("Exit check part 1 failure: one or both deployments returned nil")
	}
	if deploys[0].ID == deploys[1].ID {
		t.Fatalf("Exit check part 1 failure: deployments share same ID: %s", deploys[0].ID)
	}

	dep1, err1 := depRepo.GetByID(ctx, deploys[0].ID)
	dep2, err2 := depRepo.GetByID(ctx, deploys[1].ID)
	if err1 != nil || err2 != nil {
		t.Fatalf("failed to retrieve stored deployments: %v / %v", err1, err2)
	}
	if dep1.Status != deployments.StatusRunning || dep2.Status != deployments.StatusRunning {
		t.Fatalf("Exit check part 1 failure: expected both deployments RUNNING, got %s and %s",
			dep1.Status, dep2.Status)
	}

	t.Logf("Exit Check Part 1 SUCCESS: Two concurrent deploys to same project converged cleanly without corrupted state (Deployments: %s, %s)!",
		dep1.ID, dep2.ID)

	// =========================================================================
	// PART 2: Fire the same RunContainer request 10x in parallel
	// =========================================================================
	t.Log("=== Testing Exit Check Part 2: 10x Parallel RunContainer with Same Instance ID ===")

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
		t.Fatalf("failed to connect to worker gRPC: %v", err)
	}
	defer conn.Close()

	workerClient := proto.NewWorkerServiceClient(conn)

	const parallelCalls = 10
	const targetInstanceID = "inst-phase7-exit-check-parallel-10x"
	const targetImage = "postgres:16-alpine"

	var wgRun sync.WaitGroup
	var nonDuplicateCount int32
	var duplicateCount int32
	containerIDs := make([]string, parallelCalls)
	runGate := make(chan struct{})

	for i := 0; i < parallelCalls; i++ {
		wgRun.Add(1)
		go func(idx int) {
			defer wgRun.Done()
			<-runGate // fire all 10 calls at the exact same instant

			resp, rpcErr := workerClient.RunContainer(ctx, &proto.RunContainerRequest{
				InstanceId:   targetInstanceID,
				DeploymentId: "dep-phase7-exit",
				Image:        targetImage,
			})
			if rpcErr != nil || resp.Error != "" {
				t.Errorf("parallel call %d failed: err=%v, respErr=%s", idx, rpcErr, resp.GetError())
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

	close(runGate)
	wgRun.Wait()

	// Invariant Checks:
	// 1. Exactly 1 container created in Docker
	if mockDocker.CreateCalls != 1 {
		t.Fatalf("Exit check part 2 failure: expected 1 Docker CreateContainer call, got %d", mockDocker.CreateCalls)
	}
	if mockDocker.StartCalls != 1 {
		t.Fatalf("Exit check part 2 failure: expected 1 Docker StartContainer call, got %d", mockDocker.StartCalls)
	}

	// 2. Exactly 1 non-duplicate, 9 duplicates
	if nonDuplicateCount != 1 {
		t.Fatalf("Exit check part 2 failure: expected 1 non-duplicate response, got %d", nonDuplicateCount)
	}
	if duplicateCount != parallelCalls-1 {
		t.Fatalf("Exit check part 2 failure: expected %d duplicate responses, got %d", parallelCalls-1, duplicateCount)
	}

	// 3. All 10 parallel callers received the exact same container ID
	expectedContainerID := containerIDs[0]
	if expectedContainerID == "" {
		t.Fatalf("expected non-empty container ID")
	}
	for i, id := range containerIDs {
		if id != expectedContainerID {
			t.Fatalf("Exit check part 2 failure: caller %d got different container ID %s != %s",
				i, id, expectedContainerID)
		}
	}

	t.Logf("Exit Check Part 2 SUCCESS: 10 parallel RunContainer calls converged to exactly 1 container (%s) with zero duplicate containers!",
		expectedContainerID)

	t.Log("=========================================================================")
	t.Log("PHASE 7 EXIT CHECK COMPLETELY SATISFIED!")
	t.Log("=========================================================================")
}
