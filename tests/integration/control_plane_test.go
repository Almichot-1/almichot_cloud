package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/grpcapi"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestControlPlane_Phase2_ExitCheck validates:
// CP-01: Worker Register accepted and stored with capacity and metadata verified across RPC.
// CP-02: Heartbeat accepted.
// CP-03: CreateDeployment triggers scheduler call and single dispatch.
// CP-04: Scheduler failure when no feasible workers surfaces cleanly as FAILED.
// DRN-01: DrainWorker sets schedulability without altering Health.
// G-06..08: Mixed scheduling priority order with DRAINING worker excluded and least-loaded selected.
func TestControlPlane_Phase2_ExitCheck(t *testing.T) {
	log := zerolog.Nop()
	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)

	mockWorkerClientFactory := deployments.NewMockWorkerClientFactory()
	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockWorkerClientFactory, log)
	cpService := grpcapi.NewControlPlaneServiceServer(reg, depService, log)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on ephemeral port: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	proto.RegisterControlPlaneServiceServer(grpcServer, cpService)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.GracefulStop()

	// Connect gRPC client to Control Plane
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to connect to CP gRPC server: %v", err)
	}
	defer conn.Close()

	client := proto.NewControlPlaneServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// -------------------------------------------------------------
	// CP-01: Worker Register accepted & stored with capacity/metadata across RPC boundary
	// -------------------------------------------------------------
	worker1Key := "worker-node-1"
	resp1, err := client.RegisterWorker(ctx, &proto.RegisterWorkerRequest{
		WorkerKey: worker1Key,
		Hostname:  "node-1",
		IpAddress: "10.0.0.1",
		GrpcPort:  9091,
		Capacity:  10,
		Labels: map[string]string{
			"region": "us-east-1",
			"tier":   "standard",
		},
	})
	if err != nil || !resp1.Success {
		t.Fatalf("failed to register worker 1: %v", err)
	}

	// CP-01 Verification: Assert worker appears in registry with capacity and metadata
	w1InReg, found := reg.Get(worker1Key)
	if !found {
		t.Fatalf("CP-01 failure: worker 1 does not appear in registry after Register RPC")
	}
	if w1InReg.Capacity != 10 {
		t.Fatalf("CP-01 failure: expected capacity 10, got %d", w1InReg.Capacity)
	}
	if w1InReg.Hostname != "node-1" || w1InReg.IPAddress != "10.0.0.1" || w1InReg.GRPCPort != 9091 {
		t.Fatalf("CP-01 failure: metadata mismatch in registry: %+v", w1InReg)
	}
	if w1InReg.Labels["region"] != "us-east-1" || w1InReg.Labels["tier"] != "standard" {
		t.Fatalf("CP-01 failure: labels mismatch: %+v", w1InReg.Labels)
	}
	t.Log("CP-01 Passed: Worker 1 successfully registered and verified in registry with capacity & metadata!")

	reg.UpdateWorkload(worker1Key, 3) // set utilization to 3

	// Register Worker 2: utilization 1
	worker2Key := "worker-node-2"
	resp2, err := client.RegisterWorker(ctx, &proto.RegisterWorkerRequest{
		WorkerKey: worker2Key,
		Hostname:  "node-2",
		IpAddress: "10.0.0.2",
		Capacity:  10,
	})
	if err != nil || !resp2.Success {
		t.Fatalf("failed to register worker 2: %v", err)
	}
	reg.UpdateWorkload(worker2Key, 1) // set utilization to 1

	// Register Worker 3: utilization 0 (will be drained)
	worker3Key := "worker-node-3"
	resp3, err := client.RegisterWorker(ctx, &proto.RegisterWorkerRequest{
		WorkerKey: worker3Key,
		Hostname:  "node-3",
		IpAddress: "10.0.0.3",
		Capacity:  10,
	})
	if err != nil || !resp3.Success {
		t.Fatalf("failed to register worker 3: %v", err)
	}

	// -------------------------------------------------------------
	// CP-02: Heartbeat RPC accepted
	// -------------------------------------------------------------
	hbResp, err := client.Heartbeat(ctx, &proto.HeartbeatRequest{
		WorkerId:  resp1.WorkerId,
		Timestamp: time.Now().Unix(),
	})
	if err != nil || !hbResp.Success {
		t.Fatalf("Heartbeat failed: %v", err)
	}
	t.Log("CP-02 Passed: Heartbeat recorded successfully")

	// -------------------------------------------------------------
	// DRN-01: DrainWorker sets schedulability to DRAINING without altering Health
	// -------------------------------------------------------------
	drainResp, err := client.DrainWorker(ctx, &proto.DrainWorkerRequest{
		WorkerId: resp3.WorkerId,
	})
	if err != nil || !drainResp.Success {
		t.Fatalf("DrainWorker failed: %v", err)
	}
	if drainResp.State != "DRAINING" {
		t.Fatalf("expected state DRAINING, got %s", drainResp.State)
	}
	// DRN-01 Verification: Health must stay HEALTHY
	w3InReg, _ := reg.Get(worker3Key)
	if w3InReg.Health != workers.HealthHealthy {
		t.Fatalf("DRN-01 failure: worker health altered during drain! Got %s", w3InReg.Health)
	}
	if w3InReg.Schedulable {
		t.Fatalf("DRN-01 failure: expected Schedulable=false")
	}
	t.Log("DRN-01 Passed: DrainWorker flipped schedulability to DRAINING with health intact!")

	// -------------------------------------------------------------
	// CP-03 & G-06..08: CreateDeployment triggers scheduler and exactly one dispatch
	// -------------------------------------------------------------
	deployResp, err := client.CreateDeployment(ctx, &proto.CreateDeploymentRequest{
		ProjectId:     "project-demo",
		Image:         "nginx:alpine",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateDeployment failed: %v", err)
	}
	if deployResp.Status != "RUNNING" {
		t.Fatalf("expected deployment status RUNNING, got %s, error: %s", deployResp.Status, deployResp.Error)
	}
	if len(deployResp.InstanceIds) != 1 {
		t.Fatalf("expected 1 instance ID, got %d", len(deployResp.InstanceIds))
	}

	getResp, err := client.GetDeployment(ctx, &proto.GetDeploymentRequest{
		DeploymentId: deployResp.DeploymentId,
	})
	if err != nil || len(getResp.Instances) != 1 {
		t.Fatalf("GetDeployment failed or invalid instances: %v", err)
	}

	placedInstance := getResp.Instances[0]
	// SC-10 / G-07 check: Draining worker 3 was NEVER picked
	if placedInstance.WorkerId == resp3.WorkerId {
		t.Fatalf("SC-10 violation: Draining worker was picked!")
	}
	// SC-03 / G-08 check: Least loaded worker (worker 2) was picked
	if placedInstance.WorkerId != resp2.WorkerId {
		t.Fatalf("SC-03 violation: Expected worker 2, got %s", placedInstance.WorkerId)
	}
	// CP-03 check: Exactly 1 RunContainer dispatch
	if len(mockWorkerClientFactory.Dispatched) != 1 {
		t.Fatalf("CP-03 failure: expected exactly 1 dispatch, got %d", len(mockWorkerClientFactory.Dispatched))
	}
	t.Log("CP-03 & G-06..08 Passed: Workload landed on worker 2; exactly 1 container dispatch!")

	// -------------------------------------------------------------
	// CP-04: Scheduler failure (no feasible worker) surfaces cleanly
	// -------------------------------------------------------------
	// Request a deployment requiring a non-existent constraint so no worker is feasible
	failResp, err := client.CreateDeployment(ctx, &proto.CreateDeploymentRequest{
		ProjectId:     "project-demo",
		Image:         "nginx:alpine",
		InstanceCount: 1,
		Labels: map[string]string{
			"impossible_constraint": "true",
		},
	})
	if err != nil {
		t.Fatalf("CreateDeployment RPC should not crash, but return structured response: %v", err)
	}
	if failResp.Status != "FAILED" {
		t.Fatalf("CP-04 failure: expected deployment status FAILED when no feasible workers, got %s", failResp.Status)
	}
	if failResp.Error == "" {
		t.Fatalf("CP-04 failure: expected non-empty error message surfacing scheduler failure")
	}

	// Verify deployment record in repository is marked FAILED, not silently dropped
	failedDep, err := depRepo.GetByID(ctx, failResp.DeploymentId)
	if err != nil {
		t.Fatalf("CP-04 failure: failed deployment record was not persisted: %v", err)
	}
	if failedDep.Status != deployments.StatusFailed {
		t.Fatalf("CP-04 failure: expected record status FAILED, got %s", failedDep.Status)
	}
	t.Logf("CP-04 Passed: Scheduler failure cleanly surfaced as FAILED with error: %s", failResp.Error)
}
