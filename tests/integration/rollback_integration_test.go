package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// trackingSandbox counts invocations to assert that rollback NEVER executes a container build.
type trackingSandbox struct {
	buildCount int
}

func (s *trackingSandbox) Build(ctx context.Context, req build.BuildRequest) (*build.BuildResult, error) {
	s.buildCount++
	return &build.BuildResult{
		ImageTag: req.ImageTag,
		Digest:   "sha256:build00000000000000000000000000000000",
		BuildLog: "success",
	}, nil
}

// TestRollback_ReusesSchedulerWithoutRebuildOrRepullByTag tests the call-path guarantees:
// 1. Rollback selects the previous known-good release by recorded digest (§27.2, Gate G-30).
// 2. Rollback NEVER triggers a rebuild or re-pull by mutable tag (buildCount == 0).
// 3. Rollback reuses the exact same scheduler and worker dispatch pipeline (no parallel code path).
// 4. Load balancer routes traffic to restored release; old containers stopped; old deployment marked ROLLED_BACK.
// 5. Lifecycle events (DEPLOYMENT_ROLLED_BACK) are recorded.
func TestRollback_ReusesSchedulerWithoutRebuildOrRepullByTag(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	relRepo := deployments.NewMemoryReleaseRepository()
	eventRepo := deployments.NewMemoryEventRepository()
	workerRepo := workers.NewMemoryWorkerRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	clientFactory := deployments.NewMockWorkerClientFactory()

	// Register 2 worker nodes
	_, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-worker-1",
		Hostname:  "worker-1.local",
		IPAddress: "10.0.0.1",
		GRPCPort:  50051,
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker 1: %v", err)
	}

	_, err = reg.Register(ctx, workers.RegisterParams{
		WorkerKey: "node-worker-2",
		Hostname:  "worker-2.local",
		IPAddress: "10.0.0.2",
		GRPCPort:  50051,
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker 2: %v", err)
	}

	sandbox := &trackingSandbox{}
	builder := build.NewOrchestrator(sandbox, log)
	regClient := registry.NewMemoryRegistry(log)
	router := loadbalancer.NewRouter(log)

	svc := deployments.NewService(depRepo, instRepo, reg, sched, clientFactory, log)
	svc.SetBuildAndRegistry(builder, regClient, router)
	svc.SetReleaseRepo(relRepo)
	svc.SetEventRepo(eventRepo)

	projectID := "proj-rollback-integ"

	// -------------------------------------------------------------
	// 1. Release v17 (known-good production release)
	// -------------------------------------------------------------
	digestV17 := "sha256:1717171717171717171717171717171717171717171717171717171717171717"
	sigV17 := "sig-cosign-v17-valid"

	depV17, instancesV17, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "registry.nebula.local/proj-rollback-integ:v17",
		InstanceCount: 2,
		Ports: []deployments.PortSpec{
			{ContainerPort: 8080, HostPort: 18080, Protocol: "tcp"},
		},
	})
	if err != nil {
		t.Fatalf("deploy v17: %v", err)
	}

	// Persist release record for v17
	relV17 := &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV17.ID,
		Version:      "v17",
		ImageRef:     "registry.nebula.local/proj-rollback-integ:v17",
		ImageDigest:  digestV17,
		Signature:    sigV17,
		CreatedAt:    time.Now().Add(-30 * time.Minute),
	}
	if err := relRepo.Create(ctx, relV17); err != nil {
		t.Fatalf("create release v17: %v", err)
	}

	// Verify router has targets for v17
	targetsBefore := router.GetTargets(projectID)
	if len(targetsBefore) != 2 {
		t.Fatalf("expected 2 router targets for v17, got %d", len(targetsBefore))
	}

	// -------------------------------------------------------------
	// 2. Release v18 (bad deployment that causes issues)
	// -------------------------------------------------------------
	digestV18 := "sha256:1818181818181818181818181818181818181818181818181818181818181818"
	depV18, _, err := svc.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		Image:         "registry.nebula.local/proj-rollback-integ:v18",
		InstanceCount: 2,
		Ports: []deployments.PortSpec{
			{ContainerPort: 8080, HostPort: 18081, Protocol: "tcp"},
		},
	})
	if err != nil {
		t.Fatalf("deploy v18: %v", err)
	}

	// Stop superseded v17 deployment
	if _, err := svc.StopDeployment(ctx, depV17.ID); err != nil {
		t.Fatalf("stop v17: %v", err)
	}

	relV18 := &deployments.Release{
		ID:           uuid.New().String(),
		ProjectID:    projectID,
		DeploymentID: depV18.ID,
		Version:      "v18",
		ImageRef:     "registry.nebula.local/proj-rollback-integ:v18",
		ImageDigest:  digestV18,
		CreatedAt:    time.Now().Add(-5 * time.Minute),
	}
	if err := relRepo.Create(ctx, relV18); err != nil {
		t.Fatalf("create release v18: %v", err)
	}

	// Reset tracking counters before triggering rollback
	sandbox.buildCount = 0
	initialDispatches := len(clientFactory.Dispatched)
	initialStops := len(clientFactory.Stopped)

	// -------------------------------------------------------------
	// 3. Trigger Rollback on v18 (§27.2)
	// -------------------------------------------------------------
	startRollback := time.Now()
	restoredDep, restoredInstances, err := svc.Rollback(ctx, depV18.ID)
	rollbackDuration := time.Since(startRollback)

	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// -------------------------------------------------------------
	// G-29: Zero manual intervention restore within SLA window
	// -------------------------------------------------------------
	if rollbackDuration > 5*time.Second {
		t.Errorf("expected rollback to complete within 5s SLA (G-29), took %v", rollbackDuration)
	}

	// -------------------------------------------------------------
	// G-30 & Call-path assertion: ImageDigest pinned to v17, no rebuild
	// -------------------------------------------------------------
	if sandbox.buildCount != 0 {
		t.Fatalf("expected 0 builds during rollback (no rebuild), got %d", sandbox.buildCount)
	}
	if restoredDep.ImageDigest != digestV17 {
		t.Fatalf("expected restored deployment to use recorded digest %s (G-30), got %s", digestV17, restoredDep.ImageDigest)
	}
	if restoredDep.Revision != "v17" {
		t.Errorf("expected restored revision v17, got %s", restoredDep.Revision)
	}
	if restoredDep.Status != deployments.StatusRunning {
		t.Errorf("expected restored deployment status RUNNING, got %s", restoredDep.Status)
	}

	// Assert worker received RunContainer calls carrying the recorded digest
	newDispatches := clientFactory.Dispatched[initialDispatches:]
	if len(newDispatches) != 2 {
		t.Fatalf("expected 2 new container dispatches for restored release, got %d", len(newDispatches))
	}
	for _, d := range newDispatches {
		if d.Labels["nebula.image_digest"] != digestV17 {
			t.Errorf("expected dispatched container to have digest label %s, got %s", digestV17, d.Labels["nebula.image_digest"])
		}
		if d.Labels["nebula.signature"] != sigV17 {
			t.Errorf("expected dispatched container to preserve signature %s, got %s", sigV17, d.Labels["nebula.signature"])
		}
	}

	// -------------------------------------------------------------
	// Assert old instances were stopped
	// -------------------------------------------------------------
	newStops := clientFactory.Stopped[initialStops:]
	if len(newStops) != 2 {
		t.Errorf("expected 2 stop calls for old v18 instances, got %d", len(newStops))
	}

	// -------------------------------------------------------------
	// Assert old deployment status is ROLLED_BACK
	// -------------------------------------------------------------
	oldDep, _, err := svc.GetDeployment(ctx, depV18.ID)
	if err != nil {
		t.Fatalf("get old dep: %v", err)
	}
	if oldDep.Status != deployments.StatusRolledBack {
		t.Errorf("expected old dep status ROLLED_BACK, got %s", oldDep.Status)
	}

	// -------------------------------------------------------------
	// Assert load balancer router targets updated
	// -------------------------------------------------------------
	targetsAfter := router.GetTargets(projectID)
	if len(targetsAfter) != 2 {
		t.Fatalf("expected 2 active targets in router after rollback, got %d", len(targetsAfter))
	}

	// -------------------------------------------------------------
	// Assert DEPLOYMENT_ROLLED_BACK event recorded
	// -------------------------------------------------------------
	events, err := eventRepo.ListByDeployment(ctx, depV18.ID)
	if err != nil {
		t.Fatalf("list deployment events: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected at least 1 rollback event, got 0")
	}
	rollbackEvent := events[0]
	if rollbackEvent.EventType != "DEPLOYMENT_ROLLED_BACK" {
		t.Errorf("expected event_type DEPLOYMENT_ROLLED_BACK, got %s", rollbackEvent.EventType)
	}
	if rollbackEvent.Metadata["target_digest"] != digestV17 {
		t.Errorf("expected event metadata target_digest %s, got %v", digestV17, rollbackEvent.Metadata["target_digest"])
	}
	_ = instancesV17
	_ = restoredInstances
}
