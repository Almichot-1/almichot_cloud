package ha_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/ha"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

type haClusterNode struct {
	id         string
	depRepo    deployments.DeploymentRepository
	instRepo   deployments.InstanceRepository
	workerRepo workers.WorkerRepository
	reg        *workers.Registry
	sched      *scheduler.Scheduler
	depService *deployments.Service
	reconciler *reconcile.Reconciler
	elector    *ha.Elector
	reconciled atomic.Bool
	promotedAt time.Time
}

func newHAClusterNode(t *testing.T, id string, lockProvider ha.AdvisoryLockProvider, sharedWorkerRepo workers.WorkerRepository, sharedDepRepo deployments.DeploymentRepository, sharedInstRepo deployments.InstanceRepository) *haClusterNode {
	log := zerolog.Nop()

	reg := workers.NewRegistry(sharedWorkerRepo, log)
	sched := scheduler.NewScheduler(reg, sharedInstRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()
	depService := deployments.NewService(sharedDepRepo, sharedInstRepo, reg, sched, mockFactory, log)

	sandbox := build.NewEphemeralSandbox(log)
	orchestrator := build.NewOrchestrator(sandbox, log)
	regClient := registry.NewMemoryRegistry(log)
	router := loadbalancer.NewRouter(log)
	depService.SetBuildAndRegistry(orchestrator, regClient, router)

	reconciler := reconcile.NewReconciler(reg, sharedDepRepo, sharedInstRepo, sched, mockFactory, log)
	reconciler.SetRouter(router)

	node := &haClusterNode{
		id:         id,
		depRepo:    sharedDepRepo,
		instRepo:   sharedInstRepo,
		workerRepo: sharedWorkerRepo,
		reg:        reg,
		sched:      sched,
		depService: depService,
		reconciler: reconciler,
	}

	elector := ha.NewElector(ha.ElectorConfig{
		LockID:       ha.DefaultAdvisoryLockID,
		OwnerID:      id,
		PollInterval: 15 * time.Millisecond,
		RenewTimeout: 40 * time.Millisecond,
		LockProvider: lockProvider,
		Log:          log,
	})

	elector.OnPromoted(func(ctx context.Context) {
		node.promotedAt = time.Now()
		// §20.3 Failover reconciliation sequence:
		// Rebuild desired state from DB, reconcile actual state, then resume normal scheduling
		_, _ = reconciler.ReconcileOnce(ctx)
		node.reconciled.Store(true)
	})

	node.elector = elector
	depService.SetHAElector(elector)

	return node
}

func TestHA_Gate43_StandbyPromotionZeroWorkerDisruption(t *testing.T) {
	// G-43: killing the active CP promotes the standby within a bounded window with zero worker-side disruption
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lockProvider := ha.NewMemoryAdvisoryLock(500 * time.Millisecond)

	sharedWorkerRepo := workers.NewMemoryWorkerRepository()
	sharedDepRepo := deployments.NewMemoryDeploymentRepository()
	sharedInstRepo := deployments.NewMemoryInstanceRepository()

	// Register 2 multi-worker nodes
	wReg := workers.NewRegistry(sharedWorkerRepo, zerolog.Nop())
	_, _ = wReg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-1",
		Hostname:  "node-1",
		IPAddress: "192.168.1.11",
		Capacity:  10,
	})
	_, _ = wReg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-2",
		Hostname:  "node-2",
		IPAddress: "192.168.1.12",
		Capacity:  10,
	})

	// Spin up CP-1 (Active) and CP-2 (Hot Standby)
	cp1 := newHAClusterNode(t, "cp-1", lockProvider, sharedWorkerRepo, sharedDepRepo, sharedInstRepo)
	cp2 := newHAClusterNode(t, "cp-2", lockProvider, sharedWorkerRepo, sharedDepRepo, sharedInstRepo)

	cp1.elector.Start(ctx)
	deadlineG43 := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadlineG43) {
		if cp1.elector.IsLeader() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cp1.elector.IsLeader() {
		t.Fatal("Expected cp-1 to be active leader")
	}

	cp2.elector.Start(ctx)
	time.Sleep(20 * time.Millisecond)
	if cp2.elector.IsLeader() {
		t.Fatal("Expected cp-2 to be standby")
	}

	// Deploy a workload on CP-1
	dep, insts, err := cp1.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "live-proj",
		Image:         "registry.nebula/app:v1",
		InstanceCount: 2,
	})
	if err != nil {
		t.Fatalf("Failed to create deployment on cp-1: %v", err)
	}

	if len(insts) != 2 || dep.Status != deployments.StatusRunning {
		t.Fatalf("Expected 2 running instances, got %d (status=%s)", len(insts), dep.Status)
	}

	// Assert Standby cannot accept scheduling calls while CP-1 is active
	_, _, err = cp2.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "blocked-proj",
		Image:         "registry.nebula/app:v1",
		InstanceCount: 1,
	})
	if err == nil || !errors.Is(err, ha.ErrNotLeader) {
		t.Fatalf("Expected ErrNotLeader on standby CP-2, got: %v", err)
	}

	// KILL ACTIVE CP-1 (simulate sudden crash / process termination)
	killStartTime := time.Now()
	cp1.elector.Stop()

	// Measure failover time until CP-2 promotes to active leader
	failoverDeadline := time.Now().Add(500 * time.Millisecond)
	promoted := false
	for time.Now().Before(failoverDeadline) {
		if cp2.elector.IsLeader() {
			promoted = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	failoverDuration := time.Since(killStartTime)
	if !promoted {
		t.Fatalf("G-43 VIOLATION: Standby CP-2 failed to promote within bounded window (elapsed=%v)", failoverDuration)
	}

	// Zero Worker Disruption Invariant:
	// 1. Confirm running containers on workers remain active in storage
	inst1, err := sharedInstRepo.GetByID(ctx, insts[0].ID)
	if err != nil || inst1.Status != "RUNNING" {
		t.Fatalf("G-43 VIOLATION: instance 1 disrupted by failover: status=%s, err=%v", inst1.Status, err)
	}
	inst2, err := sharedInstRepo.GetByID(ctx, insts[1].ID)
	if err != nil || inst2.Status != "RUNNING" {
		t.Fatalf("G-43 VIOLATION: instance 2 disrupted by failover: status=%s, err=%v", inst2.Status, err)
	}

	// 2. Workers do not need to re-register: workers remain registered and healthy
	wList, err := sharedWorkerRepo.List(ctx)
	if err != nil || len(wList) != 2 {
		t.Fatalf("G-43 VIOLATION: workers were lost during failover: count=%d, err=%v", len(wList), err)
	}

	// 3. Confirm CP-2 completed §20.3 reconciliation upon promotion
	time.Sleep(20 * time.Millisecond)
	if !cp2.reconciled.Load() {
		t.Fatal("G-43 VIOLATION: newly promoted leader did not execute §20.3 reconciliation pass!")
	}

	// 4. Confirm newly promoted CP-2 can now schedule deployments
	depNew, instsNew, err := cp2.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "post-failover-proj",
		Image:         "registry.nebula/app:v2",
		InstanceCount: 1,
	})
	if err != nil || len(instsNew) != 1 || depNew.Status != deployments.StatusRunning {
		t.Fatalf("CP-2 failed to schedule post-failover deployment: %v", err)
	}

	t.Logf("✅ G-43 PASSED: killing active CP promoted standby in %v with zero worker disruption (containers running, workers preserved)", failoverDuration)
}

func TestHA_Gate44_NoSplitBrainSchedulingExclusion(t *testing.T) {
	// G-44: no split-brain — only one CP instance ever issues scheduling decisions at a time
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lockProvider := ha.NewMemoryAdvisoryLock(150 * time.Millisecond)

	sharedWorkerRepo := workers.NewMemoryWorkerRepository()
	sharedDepRepo := deployments.NewMemoryDeploymentRepository()
	sharedInstRepo := deployments.NewMemoryInstanceRepository()

	wReg := workers.NewRegistry(sharedWorkerRepo, zerolog.Nop())
	_, _ = wReg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-1",
		Hostname:  "node-1",
		IPAddress: "192.168.1.11",
		Capacity:  20,
	})

	cp1 := newHAClusterNode(t, "cp-1", lockProvider, sharedWorkerRepo, sharedDepRepo, sharedInstRepo)
	cp2 := newHAClusterNode(t, "cp-2", lockProvider, sharedWorkerRepo, sharedDepRepo, sharedInstRepo)

	cp1.elector.Start(ctx)
	deadlineG44 := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadlineG44) {
		if cp1.elector.IsLeader() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cp1.elector.IsLeader() {
		t.Fatal("Expected cp-1 to start as leader")
	}

	cp2.elector.Start(ctx)

	// Perform multiple deployments on CP-1
	for i := 0; i < 3; i++ {
		_, _, err := cp1.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
			ProjectID:     fmt.Sprintf("proj-cp1-%d", i),
			Image:         "registry.nebula/app:v1",
			InstanceCount: 1,
		})
		if err != nil {
			t.Fatalf("cp-1 deploy failed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Deliberately induce network partition between CP-1 and Postgres
	// (Simulate network disconnect without process exit)
	lockProvider.Partition("cp-1", true)

	// Wait for CP-1 to lose lock renewal, step down, and for CP-2 to promote
	time.Sleep(200 * time.Millisecond)

	if cp1.elector.IsLeader() {
		t.Fatal("G-44 VIOLATION: partitioned CP-1 did not step down!")
	}
	if !cp2.elector.IsLeader() {
		t.Fatal("G-44 VIOLATION: CP-2 was not promoted after CP-1 partition!")
	}

	// Perform deployments on CP-2
	for i := 0; i < 3; i++ {
		_, _, err := cp2.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
			ProjectID:     fmt.Sprintf("proj-cp2-%d", i),
			Image:         "registry.nebula/app:v2",
			InstanceCount: 1,
		})
		if err != nil {
			t.Fatalf("cp-2 deploy failed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify Split-Brain Invariant:
	// The decision windows of CP-1 and CP-2 must be strictly disjoint.
	// max(CP1 decisions) < min(CP2 decisions)
	decisions1 := cp1.elector.GetDecisions()
	decisions2 := cp2.elector.GetDecisions()

	if len(decisions1) == 0 || len(decisions2) == 0 {
		t.Fatalf("Expected decisions from both CPs: d1=%d, d2=%d", len(decisions1), len(decisions2))
	}

	maxCP1 := decisions1[0].Timestamp
	for _, d := range decisions1 {
		if d.Timestamp.After(maxCP1) {
			maxCP1 = d.Timestamp
		}
	}

	minCP2 := decisions2[0].Timestamp
	for _, d := range decisions2 {
		if d.Timestamp.Before(minCP2) {
			minCP2 = d.Timestamp
		}
	}

	if !maxCP1.Before(minCP2) {
		t.Fatalf("G-44 VIOLATION: Split-brain detected! Scheduling windows overlap: max(CP1)=%v, min(CP2)=%v", maxCP1, minCP2)
	}

	// Attempting to schedule on partitioned CP-1 must be strictly rejected
	_, _, err := cp1.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "split-test",
		Image:         "registry.nebula/app:v1",
		InstanceCount: 1,
	})
	if err == nil || !errors.Is(err, ha.ErrNotLeader) {
		t.Fatalf("G-44 VIOLATION: partitioned CP-1 allowed scheduling! err=%v", err)
	}

	t.Logf("✅ G-44 PASSED: zero split-brain overlap verified: max(CP1)=%v < min(CP2)=%v with strict fencing", maxCP1.Format(time.RFC3339Nano), minCP2.Format(time.RFC3339Nano))
}

func TestHA_RepeatedMidReconciliationFailover(t *testing.T) {
	// Repeated-failover test: a failover happens mid-reconciliation itself → confirm reconciliation is idempotent and converges
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lockProvider := ha.NewMemoryAdvisoryLock(300 * time.Millisecond)

	sharedWorkerRepo := workers.NewMemoryWorkerRepository()
	sharedDepRepo := deployments.NewMemoryDeploymentRepository()
	sharedInstRepo := deployments.NewMemoryInstanceRepository()

	wReg := workers.NewRegistry(sharedWorkerRepo, zerolog.Nop())
	_, _ = wReg.Register(ctx, workers.RegisterParams{
		WorkerKey: "worker-1",
		Hostname:  "node-1",
		IPAddress: "192.168.1.11",
		Capacity:  10,
	})

	cp1 := newHAClusterNode(t, "cp-1", lockProvider, sharedWorkerRepo, sharedDepRepo, sharedInstRepo)
	cp2 := newHAClusterNode(t, "cp-2", lockProvider, sharedWorkerRepo, sharedDepRepo, sharedInstRepo)

	cp1.elector.Start(ctx)

	// cp-1 must win leadership uncontested before cp-2 joins (mirrors G-43/G-44 setup)
	deadlineLeader := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadlineLeader) {
		if cp1.elector.IsLeader() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cp1.elector.IsLeader() {
		t.Fatal("Expected cp-1 to be active leader before mid-reconciliation failover")
	}

	cp2.elector.Start(ctx)
	time.Sleep(20 * time.Millisecond)

	// Create deployment
	_, _, err := cp1.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     "mid-reconcile-proj",
		Image:         "registry.nebula/app:v1",
		InstanceCount: 1,
	})
	if err != nil {
		t.Fatalf("Create deployment failed: %v", err)
	}

	// Trigger concurrent reconciliation on CP-1 while simultaneously killing CP-1
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = cp1.reconciler.ReconcileOnce(ctx)
	}()

	time.Sleep(1 * time.Millisecond)
	cp1.elector.Stop() // Kill CP-1 mid-reconciliation
	wg.Wait()

	// Wait for CP-2 to take over leadership and complete its promotion reconciliation
	time.Sleep(100 * time.Millisecond)

	if !cp2.elector.IsLeader() {
		t.Fatal("CP-2 should have assumed leadership")
	}

	// Run another reconciliation pass on CP-2 to assert convergence
	actions, err := cp2.reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("CP-2 post-failover reconciliation failed: %v", err)
	}

	// Should be zero unhandled errors or stuck states
	if len(actions.Errors) > 0 {
		t.Fatalf("Post-failover reconciliation reported errors: %v", actions.Errors)
	}

	t.Log("✅ Repeated mid-reconciliation failover passed: state converged idempotently")
}
