package backup_test

import (
	"context"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/storage/backup"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

func setupTestRepos() (
	deployments.DeploymentRepository,
	deployments.InstanceRepository,
	projects.ProjectRepository,
	deployments.ReleaseRepository,
	deployments.EventRepository,
	workers.WorkerRepository,
) {
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	projectRepo := projects.NewMemoryProjectRepository()
	releaseRepo := deployments.NewMemoryReleaseRepository()
	eventRepo := deployments.NewMemoryEventRepository()
	workerRepo := workers.NewMemoryWorkerRepository()
	return depRepo, instRepo, projectRepo, releaseRepo, eventRepo, workerRepo
}

func TestBackup_CreateBaseBackupAndRestore(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo, instRepo, projectRepo, releaseRepo, eventRepo, workerRepo := setupTestRepos()

	mgr := backup.NewMemoryBackupManager(depRepo, instRepo, projectRepo, releaseRepo, eventRepo, workerRepo, log)

	// Seed initial state
	p1 := &projects.Project{ID: "proj-1", Name: "Alpha"}
	_ = projectRepo.Create(ctx, p1)

	d1 := &deployments.Deployment{
		ID:            "dep-1",
		ProjectID:     "proj-1",
		Image:         "registry.nebula/alpha:v1",
		Status:        deployments.StatusRunning,
		InstanceCount: 2,
	}
	_ = depRepo.Create(ctx, d1)

	inst1 := &deployments.Instance{ID: "inst-1", DeploymentID: "dep-1", WorkerID: "worker-1", Status: "RUNNING"}
	inst2 := &deployments.Instance{ID: "inst-2", DeploymentID: "dep-1", WorkerID: "worker-2", Status: "RUNNING"}
	_ = instRepo.Create(ctx, inst1)
	_ = instRepo.Create(ctx, inst2)

	// 1. Take Base Backup
	baseBackup, err := mgr.CreateBaseBackup(ctx)
	if err != nil {
		t.Fatalf("failed to create base backup: %v", err)
	}
	if baseBackup.ID == "" || baseBackup.BaseLSN != 0 {
		t.Fatalf("unexpected backup metadata: %+v", baseBackup)
	}

	// 2. Wipe repositories completely (simulate disaster / storage loss)
	_ = projectRepo.(*projects.MemoryProjectRepository).Wipe(ctx)
	_ = depRepo.(*deployments.MemoryDeploymentRepository).Wipe(ctx)
	_ = instRepo.(*deployments.MemoryInstanceRepository).Wipe(ctx)

	// Verify wiped
	list, _ := depRepo.List(ctx, "")
	if len(list) != 0 {
		t.Fatalf("expected wiped depRepo, got %d", len(list))
	}

	// 3. Restore from Base Backup
	if err := mgr.Restore(ctx, baseBackup, nil); err != nil {
		t.Fatalf("failed to restore from base backup: %v", err)
	}

	// 4. Verify full restoration
	restoredP1, err := projectRepo.GetByID(ctx, "proj-1")
	if err != nil || restoredP1.Name != "Alpha" {
		t.Fatalf("restored project mismatch: %+v, err=%v", restoredP1, err)
	}

	restoredD1, err := depRepo.GetByID(ctx, "dep-1")
	if err != nil || restoredD1.Status != deployments.StatusRunning {
		t.Fatalf("restored deployment mismatch: %+v, err=%v", restoredD1, err)
	}

	allInsts, err := instRepo.ListAll(ctx)
	if err != nil || len(allInsts) != 2 {
		t.Fatalf("restored instances mismatch: got %d, err=%v", len(allInsts), err)
	}
}

func TestBackup_ContinuousWALArchiving_And_PointInTimeRecovery(t *testing.T) {
	ctx := context.Background()
	log := zerolog.Nop()

	depRepo, instRepo, projectRepo, releaseRepo, eventRepo, workerRepo := setupTestRepos()
	mgr := backup.NewMemoryBackupManager(depRepo, instRepo, projectRepo, releaseRepo, eventRepo, workerRepo, log)

	// T0: Initial Project & Deployment D1
	p1 := &projects.Project{ID: "proj-1", Name: "Alpha"}
	_ = projectRepo.Create(ctx, p1)

	d1 := &deployments.Deployment{
		ID:            "dep-1",
		ProjectID:     "proj-1",
		Image:         "registry.nebula/alpha:v1",
		Status:        deployments.StatusRunning,
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, d1)

	// Take Base Backup at T0
	baseBackup, err := mgr.CreateBaseBackup(ctx)
	if err != nil {
		t.Fatalf("failed to create base backup: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	// T1: Live writes after Base Backup, recorded to continuous WAL archive (§30)
	d2 := &deployments.Deployment{
		ID:            "dep-2",
		ProjectID:     "proj-1",
		Image:         "registry.nebula/alpha:v2",
		Status:        deployments.StatusRunning,
		InstanceCount: 1,
	}
	_ = depRepo.Create(ctx, d2)
	_ = mgr.RecordMutation(ctx, "deployments", "INSERT", d2.ID, d2)

	instD2 := &deployments.Instance{ID: "inst-d2", DeploymentID: "dep-2", WorkerID: "worker-1", Status: "RUNNING"}
	_ = instRepo.Create(ctx, instD2)
	_ = mgr.RecordMutation(ctx, "instances", "INSERT", instD2.ID, instD2)

	pitrTarget := time.Now().UTC()

	// 1. Wipe primary storage
	_ = depRepo.(*deployments.MemoryDeploymentRepository).Wipe(ctx)
	_ = instRepo.(*deployments.MemoryInstanceRepository).Wipe(ctx)

	// 2. Point-in-time Restore: Base Backup + replay WAL up to pitrTarget
	if err := mgr.Restore(ctx, baseBackup, &pitrTarget); err != nil {
		t.Fatalf("PITR restore failed: %v", err)
	}

	// 3. Confirm both D1 (from base backup) AND D2 (replayed from continuous WAL) exist
	deps, err := depRepo.List(ctx, "")
	if err != nil || len(deps) != 2 {
		t.Fatalf("expected 2 deployments after PITR replay, got %d (err=%v)", len(deps), err)
	}

	insts, err := instRepo.ListAll(ctx)
	if err != nil || len(insts) != 1 {
		t.Fatalf("expected 1 replayed instance, got %d", len(insts))
	}
}
