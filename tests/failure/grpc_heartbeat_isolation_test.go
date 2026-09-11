package failure

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// timedOutWorkerClient simulates a worker whose gRPC command port is hung or partitioned,
// causing all RPC calls to block until deadline exceeded (§14.3).
type timedOutWorkerClient struct {
	proto.WorkerServiceClient
	delay time.Duration
}

func (c *timedOutWorkerClient) RunContainer(ctx context.Context, req *proto.RunContainerRequest, opts ...grpc.CallOption) (*proto.RunContainerResponse, error) {
	select {
	case <-time.After(c.delay):
		return nil, status.Error(codes.DeadlineExceeded, "gRPC command port timed out: network partition / socket frozen (§14.3)")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *timedOutWorkerClient) StopContainer(ctx context.Context, req *proto.StopContainerRequest, opts ...grpc.CallOption) (*proto.StopContainerResponse, error) {
	select {
	case <-time.After(c.delay):
		return nil, status.Error(codes.DeadlineExceeded, "gRPC command port timed out (§14.3)")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type timedOutFactory struct {
	client *timedOutWorkerClient
}

func (f *timedOutFactory) GetClient(ctx context.Context, worker *workers.Worker) (deployments.WorkerClient, error) {
	return f.client, nil
}

// TestFailure_GRPCTargetTimeout_WorkerRemainsHealthyViaHeartbeat isolates the distributed
// systems invariant defined in §14.3: an RPC failure or timeout alone does NOT prove a worker
// is dead; worker health is decoupled and governed exclusively by heartbeats.
func TestFailure_GRPCTargetTimeout_WorkerRemainsHealthyViaHeartbeat(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	// 1. Initialize Control Plane repositories and Worker Registry
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	workerRegistry := workers.NewRegistry(workerRepo, log)

	workerKey := "worker-isolated-grpc-01"
	_, err := workerRegistry.Register(ctx, workers.RegisterParams{
		WorkerKey: workerKey,
		Hostname:  "worker-isolated.internal",
		IPAddress: "10.0.0.15",
		GRPCPort:  9090,
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// 2. Start a continuous heartbeat loop on the separate health-check path (:8081)
	stopHeartbeats := make(chan struct{})
	var hbWg sync.WaitGroup
	hbWg.Add(1)
	go func() {
		defer hbWg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeats:
				return
			case <-ticker.C:
				_ = workerRegistry.Heartbeat(ctx, workerKey)
			}
		}
	}()

	t.Cleanup(func() {
		close(stopHeartbeats)
		hbWg.Wait()
	})

	// Wait for first heartbeat to establish baseline
	time.Sleep(100 * time.Millisecond)

	// Verify worker is HEALTHY initially
	wInit, ok := workerRegistry.Get(workerKey)
	if !ok || wInit.Health != workers.HealthHealthy {
		t.Fatalf("expected worker to be HEALTHY initially, got %v", wInit.Health)
	}

	// 3. Configure gRPC client whose command port is frozen / partitioned
	hungClient := &timedOutWorkerClient{
		delay: 150 * time.Millisecond,
	}
	factory := &timedOutFactory{client: hungClient}

	sched := scheduler.NewScheduler(workerRegistry, instRepo.CountByWorkerForDeployment, log)
	svc := deployments.NewService(depRepo, instRepo, workerRegistry, sched, factory, log)

	memReg := registry.NewMemoryRegistry(log)
	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	router := loadbalancer.NewRouter(log)
	svc.SetBuildAndRegistry(orchestrator, memReg, router)

	srcDir := filepath.Join(t.TempDir(), "app-grpc-timeout")
	_ = os.MkdirAll(srcDir, 0755)
	_ = os.WriteFile(filepath.Join(srcDir, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"sleep\", \"10\"]"), 0644)

	// 4. Trigger deployment: RunContainer RPC call will hit deadline / timeout
	deployCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dep, _, deployErr := svc.CreateAndDeploy(deployCtx, deployments.CreateDeploymentParams{
		ProjectID:  "proj-grpc-timeout-isolation",
		SourcePath: srcDir,
	})

	// 5. Assert deployment RPC failed as expected
	if deployErr == nil {
		t.Fatalf("expected CreateAndDeploy to fail due to gRPC command timeout")
	}
	if !strings.Contains(deployErr.Error(), "timed out") && !strings.Contains(deployErr.Error(), "DeadlineExceeded") {
		t.Fatalf("expected deadline exceeded error from gRPC call, got: %v", deployErr)
	}

	if dep.Status != deployments.StatusFailed {
		t.Fatalf("expected deployment to be marked FAILED, got %s", dep.Status)
	}

	// 6. §14.3 CRITICAL ASSERTION: The worker must REMAIN HEALTHY and Schedulable!
	// RPC failure alone does not prove the worker is dead when heartbeats continue.
	workerAfter, ok := workerRegistry.Get(workerKey)
	if !ok {
		t.Fatalf("worker disappeared from registry")
	}

	if workerAfter.Health != workers.HealthHealthy {
		t.Fatalf("§14.3 VIOLATION: worker marked %s after RPC timeout alone; must remain HEALTHY", workerAfter.Health)
	}
	if !workerAfter.Schedulable {
		t.Fatalf("worker marked unschedulable after RPC failure alone; must remain schedulable")
	}
	if workerAfter.State != workers.StateReady {
		t.Fatalf("worker state changed to %s; must remain READY", workerAfter.State)
	}
}
