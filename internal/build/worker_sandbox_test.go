package build

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

func TestBuildWorkerNode_RegistrationAndHeartbeat(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)

	node := NewBuildWorkerNode(BuildWorkerNodeConfig{
		WorkerKey: "build-worker-01",
		Hostname:  "bld-01.internal",
		IPAddress: "10.0.1.101",
		Port:      9091,
		Capacity:  4,
	}, log)

	w, err := node.Register(context.Background(), reg)
	if err != nil {
		t.Fatalf("failed to register build worker node: %v", err)
	}

	if w.Capability() != workers.CapabilityBuild {
		t.Fatalf("expected capability %q, got %q", workers.CapabilityBuild, w.Capability())
	}
	if !w.IsBuildWorker() {
		t.Fatalf("expected IsBuildWorker to be true")
	}

	// Verify it shows up in registry under build workers
	bldWorkers := reg.ListBuildWorkers()
	if len(bldWorkers) != 1 || bldWorkers[0].WorkerKey != "build-worker-01" {
		t.Fatalf("expected 1 build worker with key build-worker-01, got %v", bldWorkers)
	}

	// Start heartbeats and verify they record
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.StartHeartbeat(ctx, 20*time.Millisecond)

	time.Sleep(60 * time.Millisecond)
	fetched, ok := reg.Get("build-worker-01")
	if !ok {
		t.Fatalf("failed to get build worker: not found")
	}
	if fetched.LastBeatAt == nil {
		t.Fatalf("expected LastBeatAt to be updated by heartbeat")
	}

	node.Stop()
	if !node.IsKilled() {
		t.Fatalf("expected node to be marked killed after Stop")
	}
}

func TestWorkerSandbox_ExecutionOnDedicatedWorker(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	sched := scheduler.NewScheduler(reg, nil, log)
	pool := NewLocalBuildWorkerPool()

	// Register a runtime worker and a build worker
	_, err := reg.Register(context.Background(), workers.RegisterParams{
		WorkerKey: "runtime-01",
		Hostname:  "run-01",
		IPAddress: "10.0.2.1",
		GRPCPort:  9090,
		Capacity:  10,
		Labels:    map[string]string{"capability": workers.CapabilityRuntime},
	})
	if err != nil {
		t.Fatalf("failed to register runtime worker: %v", err)
	}

	bldNode := NewBuildWorkerNode(BuildWorkerNodeConfig{
		WorkerKey: "build-01",
		Hostname:  "bld-01",
		IPAddress: "10.0.3.1",
		Port:      9091,
		Capacity:  2,
	}, log)
	_, err = bldNode.Register(context.Background(), reg)
	if err != nil {
		t.Fatalf("failed to register build worker: %v", err)
	}
	pool.RegisterWorker("build-01", bldNode)

	sandbox := NewWorkerSandbox(sched, reg, pool, log)

	// Prepare source dir
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "Dockerfile"), []byte("FROM alpine:latest\nCMD [\"echo\", \"ok\"]"), 0644)

	plan := &BuildPlan{
		Strategy:          StrategyDockerfile,
		DockerfileContent: "FROM alpine:latest\nCMD [\"echo\", \"ok\"]",
	}

	res, err := sandbox.Build(context.Background(), BuildRequest{
		ProjectID: "proj-dedicated-build",
		SourceDir: tmpDir,
		Plan:      plan,
		ImageTag:  "nebula/dedicated-test:v1",
	})
	if err != nil {
		t.Fatalf("sandbox build on dedicated worker failed: %v", err)
	}

	if res.Digest == "" || !strings.HasPrefix(res.Digest, "sha256:") {
		t.Fatalf("expected valid sha256 digest, got: %s", res.Digest)
	}
	if !strings.Contains(res.BuildLog, "Successfully built nebula/dedicated-test:v1 on worker build-01") {
		t.Fatalf("expected build log to show execution on build-01, got: %s", res.BuildLog)
	}

	// Verify workload returned to 0 after completion
	w, _ := reg.Get("build-01")
	if w.ActiveWorkloads != 0 {
		t.Fatalf("expected active workloads to be 0 after build finishes, got %d", w.ActiveWorkloads)
	}
}

func TestWorkerSandbox_NoBuildWorkersAvailable(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	sched := scheduler.NewScheduler(reg, nil, log)
	pool := NewLocalBuildWorkerPool()

	// Only runtime workers registered
	_, err := reg.Register(context.Background(), workers.RegisterParams{
		WorkerKey: "runtime-only-01",
		Hostname:  "run-01",
		IPAddress: "10.0.2.1",
		GRPCPort:  9090,
		Capacity:  10,
		Labels:    map[string]string{"capability": workers.CapabilityRuntime},
	})
	if err != nil {
		t.Fatalf("failed to register runtime worker: %v", err)
	}

	sandbox := NewWorkerSandbox(sched, reg, pool, log)

	tmpDir := t.TempDir()
	_, err = sandbox.Build(context.Background(), BuildRequest{
		ProjectID: "proj-fail-no-worker",
		SourceDir: tmpDir,
		Plan:      &BuildPlan{Strategy: StrategyDockerfile},
		ImageTag:  "nebula/noworker:v1",
	})

	if err == nil {
		t.Fatalf("expected error when no build workers are available")
	}
	if !strings.Contains(err.Error(), "no build worker available") && !strings.Contains(err.Error(), "no feasible workers") {
		t.Fatalf("expected no feasible workers / no build worker available error, got: %v", err)
	}
}

func TestWorkerSandbox_WorkerKilledMidBuild(t *testing.T) {
	log := zerolog.Nop()
	repo := workers.NewMemoryWorkerRepository()
	reg := workers.NewRegistry(repo, log)
	sched := scheduler.NewScheduler(reg, nil, log)
	pool := NewLocalBuildWorkerPool()

	bldNode := NewBuildWorkerNode(BuildWorkerNodeConfig{
		WorkerKey: "build-chaos-01",
		Hostname:  "bld-chaos",
		IPAddress: "10.0.3.2",
		Port:      9091,
		Capacity:  2,
	}, log)
	_, _ = bldNode.Register(context.Background(), reg)
	pool.RegisterWorker("build-chaos-01", bldNode)

	// Inject a mid-build hook that terminates the worker node
	var hookExecuted sync.WaitGroup
	hookExecuted.Add(1)
	bldNode.SetMidBuildHook(func() {
		bldNode.Kill()
		hookExecuted.Done()
	})

	sandbox := NewWorkerSandbox(sched, reg, pool, log)

	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "Dockerfile"), []byte("FROM alpine:latest"), 0644)

	_, err := sandbox.Build(context.Background(), BuildRequest{
		ProjectID: "proj-chaos-kill",
		SourceDir: tmpDir,
		Plan:      &BuildPlan{Strategy: StrategyDockerfile},
		ImageTag:  "nebula/chaos-kill:v1",
	})

	if err == nil {
		t.Fatalf("expected build to fail when worker is killed mid-build")
	}
	if !strings.Contains(err.Error(), "terminated mid-build") {
		t.Fatalf("expected terminated mid-build error, got: %v", err)
	}
}
