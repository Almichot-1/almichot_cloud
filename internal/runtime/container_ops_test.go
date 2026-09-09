package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestInstanceTracker_LockAndCRUD(t *testing.T) {
	tracker := NewInstanceTracker()
	const id = "inst-unit-test"

	unlock := tracker.LockInstance(id)
	rec := &InstanceRecord{
		InstanceID: id,
		State:      StateStarting,
	}
	tracker.Set(rec)
	unlock()

	retrieved, ok := tracker.Get(id)
	if !ok {
		t.Fatalf("expected record to exist")
	}
	if retrieved.State != StateStarting {
		t.Fatalf("expected state %s, got %s", StateStarting, retrieved.State)
	}

	list := tracker.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 record in list, got %d", len(list))
	}

	tracker.Delete(id)
	_, ok = tracker.Get(id)
	if ok {
		t.Fatalf("expected record to be deleted")
	}
}

func TestContainerOps_RunAndStop(t *testing.T) {
	mockDocker := NewMockDockerClient()
	tracker := NewInstanceTracker()
	log := zerolog.Nop()
	ops := NewContainerOps(mockDocker, tracker, log)
	ctx := context.Background()

	// 1. Run container
	runRes, err := ops.RunContainer(ctx, RunOptions{
		InstanceID: "inst-ops-1",
		Image:      "test-image:latest",
	})
	if err != nil {
		t.Fatalf("RunContainer failed: %v", err)
	}
	if runRes.IsDuplicate {
		t.Fatalf("expected not duplicate")
	}
	if runRes.ContainerID == "" {
		t.Fatalf("expected non-empty container ID")
	}

	// 2. Duplicate RunContainer (G-09)
	runRes2, err := ops.RunContainer(ctx, RunOptions{
		InstanceID: "inst-ops-1",
		Image:      "test-image:latest",
	})
	if err != nil {
		t.Fatalf("duplicate RunContainer failed: %v", err)
	}
	if !runRes2.IsDuplicate {
		t.Fatalf("expected duplicate flag to be true")
	}
	if runRes2.ContainerID != runRes.ContainerID {
		t.Fatalf("expected identical container ID")
	}

	// 3. Status inspection
	statusRes, err := ops.GetContainerStatus(ctx, "inst-ops-1")
	if err != nil {
		t.Fatalf("GetContainerStatus failed: %v", err)
	}
	if statusRes.Status != "running" {
		t.Fatalf("expected running, got %s", statusRes.Status)
	}

	// 4. Stop container
	stopRes, err := ops.StopContainer(ctx, "inst-ops-1", 1*time.Second)
	if err != nil {
		t.Fatalf("StopContainer failed: %v", err)
	}
	if !stopRes.Success {
		t.Fatalf("expected stop success")
	}

	// 5. Status after stop
	statusRes2, err := ops.GetContainerStatus(ctx, "inst-ops-1")
	if err != nil {
		t.Fatalf("GetContainerStatus after stop failed: %v", err)
	}
	if statusRes2.Status != "stopped" {
		t.Fatalf("expected stopped, got %s", statusRes2.Status)
	}
}

func TestContainerOps_DockerFailures(t *testing.T) {
	mockDocker := NewMockDockerClient()
	tracker := NewInstanceTracker()
	log := zerolog.Nop()
	ops := NewContainerOps(mockDocker, tracker, log)
	ctx := context.Background()

	// Test create failure
	mockDocker.FailCreate = errors.New("daemon connection refused")
	res, err := ops.RunContainer(ctx, RunOptions{
		InstanceID: "inst-err-1",
		Image:      "bad-image",
	})
	if err == nil {
		t.Fatalf("expected error from failed create")
	}
	if res.Status != string(StateFailed) {
		t.Fatalf("expected state FAILED, got %s", res.Status)
	}

	// Test start failure
	mockDocker.FailCreate = nil
	mockDocker.FailStart = errors.New("permission denied")
	res2, err := ops.RunContainer(ctx, RunOptions{
		InstanceID: "inst-err-2",
		Image:      "bad-image",
	})
	if err == nil {
		t.Fatalf("expected error from failed start")
	}
	if res2.Status != string(StateFailed) {
		t.Fatalf("expected state FAILED, got %s", res2.Status)
	}
}
