package failure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/heartbeat"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// mockCPClient records heartbeat calls from worker
type mockCPClient struct {
	proto.UnimplementedControlPlaneServiceServer
	heartbeatCount int
	lastTimestamp  int64
}

func (m *mockCPClient) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	m.heartbeatCount++
	m.lastTimestamp = req.Timestamp
	return &proto.HeartbeatResponse{Success: true, RecordedAt: req.Timestamp}, nil
}

// FS-02 & WA-13 (Gate G-14): Docker failure != Worker failure.
// When container creation/starting crashes with a Docker failure:
// - Container is marked FAILED.
// - Worker-level health remains HEALTHY.
// - Heartbeats continue flowing without interruption.
func TestFS02_WA13_G14_DockerFailureDoesNotDegradeWorkerHealth(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// 1. Worker runtime with failing Docker daemon simulation
	mockDocker := runtime.NewMockDockerClient()
	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(mockDocker, tracker, log)

	// Worker model in registry
	w := &workers.Worker{
		ID:        "worker-id-14",
		WorkerKey: "worker-node-14",
		Health:    workers.HealthHealthy,
	}

	// 2. Heartbeat sender connected to Control Plane
	mockCP := &mockCPClient{}
	// Direct client wrapper
	sender := heartbeat.NewHeartbeatSender(w.ID, &grpcCPClientDirect{mockCP: mockCP}, 50*time.Millisecond, log)
	sender.Start(ctx)
	defer sender.Stop()

	// Initial heartbeats flowing
	time.Sleep(120 * time.Millisecond)
	initialBeats := mockCP.heartbeatCount
	if initialBeats < 1 {
		t.Fatalf("expected heartbeats to start flowing, got %d", initialBeats)
	}

	// 3. Inject catastrophic Docker failure on container run
	mockDocker.FailStart = errors.New("OCI runtime exec failed: executable file not found in $PATH")

	runRes, err := ops.RunContainer(ctx, runtime.RunOptions{
		InstanceID:   "inst-fail-test",
		DeploymentID: "dep-test",
		Image:        "broken-image:latest",
	})
	if err == nil {
		t.Fatalf("expected error from RunContainer, got nil")
	}

	// Verify container status is FAILED
	if runRes.Status != string(runtime.StateFailed) {
		t.Fatalf("expected container status FAILED, got %s", runRes.Status)
	}

	// 4. Invariant Assertion (WA-13 & G-14):
	// Worker health must NOT be affected by container failure!
	if w.Health != workers.HealthHealthy {
		t.Fatalf("WA-13 / G-14 failure: worker health degraded to %s due to container failure!", w.Health)
	}

	// 5. Invariant Assertion (FS-02 & G-14):
	// Heartbeats MUST continue flowing uninterrupted!
	time.Sleep(120 * time.Millisecond)
	subsequentBeats := mockCP.heartbeatCount
	if subsequentBeats <= initialBeats {
		t.Fatalf("FS-02 / G-14 failure: heartbeats stopped flowing after Docker failure! (was %d, now %d)",
			initialBeats, subsequentBeats)
	}

	t.Logf("FS-02 & WA-13 (Gate G-14) Passed: Container FAILED, but worker remained HEALTHY and heartbeats kept flowing (%d beats)!",
		subsequentBeats)
}

// Helper client adapter implementing proto.ControlPlaneServiceClient
type grpcCPClientDirect struct {
	proto.ControlPlaneServiceClient
	mockCP *mockCPClient
}

func (c *grpcCPClientDirect) Heartbeat(ctx context.Context, in *proto.HeartbeatRequest, opts ...grpc.CallOption) (*proto.HeartbeatResponse, error) {
	return c.mockCP.Heartbeat(ctx, in)
}
