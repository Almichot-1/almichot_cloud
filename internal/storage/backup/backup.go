package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

var (
	ErrBackupNotFound = errors.New("backup not found")
	ErrRestoreFailed  = errors.New("restore operation failed")
)

// WALRecord represents an individual Write-Ahead Log mutation segment (§30).
type WALRecord struct {
	LSN       uint64    `json:"lsn"`
	Timestamp time.Time `json:"timestamp"`
	Table     string    `json:"table"`
	Operation string    `json:"operation"` // "INSERT", "UPDATE", "DELETE"
	RecordID  string    `json:"record_id"`
	Payload   []byte    `json:"payload"`
}

// BaseBackup represents a point-in-time full database snapshot (§30).
type BaseBackup struct {
	ID        string            `json:"id"`
	BaseLSN   uint64            `json:"base_lsn"`
	CreatedAt time.Time         `json:"created_at"`
	Data      map[string][]byte `json:"data"` // table name -> serialized records
}

// BackupManager abstracts database base backup creation, continuous WAL archiving, and point-in-time recovery.
type BackupManager interface {
	CreateBaseBackup(ctx context.Context) (*BaseBackup, error)
	ArchiveWAL(ctx context.Context, record *WALRecord) error
	Restore(ctx context.Context, backup *BaseBackup, targetTime *time.Time) error
	ListBackups(ctx context.Context) ([]*BaseBackup, error)
}

// MemoryBackupManager implements BackupManager for testing, simulation, and in-memory environments.
type MemoryBackupManager struct {
	depRepo     deployments.DeploymentRepository
	instRepo    deployments.InstanceRepository
	projectRepo projects.ProjectRepository
	releaseRepo deployments.ReleaseRepository
	eventRepo   deployments.EventRepository
	workerRepo  workers.WorkerRepository

	currentLSN atomic.Uint64
	backups    map[string]*BaseBackup
	walLog     []*WALRecord

	log zerolog.Logger
	mu  sync.RWMutex
}

// NewMemoryBackupManager creates a new MemoryBackupManager tied to cluster repositories.
func NewMemoryBackupManager(
	depRepo deployments.DeploymentRepository,
	instRepo deployments.InstanceRepository,
	projectRepo projects.ProjectRepository,
	releaseRepo deployments.ReleaseRepository,
	eventRepo deployments.EventRepository,
	workerRepo workers.WorkerRepository,
	log zerolog.Logger,
) *MemoryBackupManager {
	return &MemoryBackupManager{
		depRepo:     depRepo,
		instRepo:    instRepo,
		projectRepo: projectRepo,
		releaseRepo: releaseRepo,
		eventRepo:   eventRepo,
		workerRepo:  workerRepo,
		backups:     make(map[string]*BaseBackup),
		walLog:      make([]*WALRecord, 0),
		log:         log.With().Str("component", "backup-manager").Logger(),
	}
}

// RecordMutation logs a mutation to the continuous WAL archive (§30).
func (m *MemoryBackupManager) RecordMutation(ctx context.Context, table, op, id string, entity any) error {
	payload, err := json.Marshal(entity)
	if err != nil {
		return fmt.Errorf("failed to serialize WAL record: %w", err)
	}

	lsn := m.currentLSN.Add(1)
	record := &WALRecord{
		LSN:       lsn,
		Timestamp: time.Now().UTC(),
		Table:     table,
		Operation: op,
		RecordID:  id,
		Payload:   payload,
	}

	return m.ArchiveWAL(ctx, record)
}

// ArchiveWAL records a WAL segment into the continuous WAL archive (§30).
func (m *MemoryBackupManager) ArchiveWAL(ctx context.Context, record *WALRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.walLog = append(m.walLog, record)
	m.log.Debug().
		Uint64("lsn", record.LSN).
		Str("table", record.Table).
		Str("op", record.Operation).
		Str("id", record.RecordID).
		Msg("continuous WAL segment archived")

	return nil
}

// CreateBaseBackup takes a full consistent snapshot of all repository tables (§30).
func (m *MemoryBackupManager) CreateBaseBackup(ctx context.Context) (*BaseBackup, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data := make(map[string][]byte)

	// Snapshot Projects
	if m.projectRepo != nil {
		projs, err := m.projectRepo.List(ctx)
		if err == nil {
			bytes, _ := json.Marshal(projs)
			data["projects"] = bytes
		}
	}

	// Snapshot Deployments
	if m.depRepo != nil {
		deps, err := m.depRepo.List(ctx, "")
		if err == nil {
			bytes, _ := json.Marshal(deps)
			data["deployments"] = bytes
		}
	}

	// Snapshot Instances
	if m.instRepo != nil {
		insts, err := m.instRepo.ListAll(ctx)
		if err == nil {
			bytes, _ := json.Marshal(insts)
			data["instances"] = bytes
		}
	}

	// Snapshot Releases
	if m.releaseRepo != nil {
		if lister, ok := m.releaseRepo.(interface {
			List(ctx context.Context, projectID string) ([]*deployments.Release, error)
		}); ok {
			rels, err := lister.List(ctx, "")
			if err == nil {
				bytes, _ := json.Marshal(rels)
				data["releases"] = bytes
			}
		} else {
			rels, err := m.releaseRepo.ListByProject(ctx, "")
			if err == nil {
				bytes, _ := json.Marshal(rels)
				data["releases"] = bytes
			}
		}
	}

	// Snapshot Events
	if m.eventRepo != nil {
		if lister, ok := m.eventRepo.(interface {
			ListAll(ctx context.Context) ([]*deployments.Event, error)
		}); ok {
			evts, err := lister.ListAll(ctx)
			if err == nil {
				bytes, _ := json.Marshal(evts)
				data["events"] = bytes
			}
		} else {
			evts, err := m.eventRepo.ListByProject(ctx, "")
			if err == nil {
				bytes, _ := json.Marshal(evts)
				data["events"] = bytes
			}
		}
	}

	// Snapshot Workers
	if m.workerRepo != nil {
		wks, err := m.workerRepo.List(ctx)
		if err == nil {
			bytes, _ := json.Marshal(wks)
			data["workers"] = bytes
		}
	}

	backupID := uuid.New().String()
	backup := &BaseBackup{
		ID:        backupID,
		BaseLSN:   m.currentLSN.Load(),
		CreatedAt: time.Now().UTC(),
		Data:      data,
	}

	m.backups[backupID] = backup
	m.log.Info().
		Str("backup_id", backupID).
		Uint64("base_lsn", backup.BaseLSN).
		Int("tables", len(data)).
		Msg("base backup created successfully")

	return backup, nil
}

// Restore restores repositories from a base backup and replays WAL segments up to targetTime (PITR) (§30).
func (m *MemoryBackupManager) Restore(ctx context.Context, backup *BaseBackup, targetTime *time.Time) error {
	if backup == nil {
		return errors.New("cannot restore from nil backup")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.log.Warn().
		Str("backup_id", backup.ID).
		Uint64("base_lsn", backup.BaseLSN).
		Msg("initiating database restore from base backup")

	wipeRepo := func(r any) {
		if w, ok := r.(interface {
			Wipe(ctx context.Context) error
		}); ok {
			_ = w.Wipe(ctx)
		}
	}

	// 1. Wipe & Restore Projects
	if m.projectRepo != nil {
		if raw, ok := backup.Data["projects"]; ok {
			var projs []*projects.Project
			_ = json.Unmarshal(raw, &projs)
			wipeRepo(m.projectRepo)
			for _, p := range projs {
				_ = m.projectRepo.Create(ctx, p)
			}
		}
	}

	// 2. Wipe & Restore Deployments
	if m.depRepo != nil {
		if raw, ok := backup.Data["deployments"]; ok {
			var deps []*deployments.Deployment
			_ = json.Unmarshal(raw, &deps)
			wipeRepo(m.depRepo)
			for _, d := range deps {
				_ = m.depRepo.Create(ctx, d)
			}
		}
	}

	// 3. Wipe & Restore Instances
	if m.instRepo != nil {
		if raw, ok := backup.Data["instances"]; ok {
			var insts []*deployments.Instance
			_ = json.Unmarshal(raw, &insts)
			wipeRepo(m.instRepo)
			for _, i := range insts {
				_ = m.instRepo.Create(ctx, i)
			}
		}
	}

	// 4. Wipe & Restore Releases
	if m.releaseRepo != nil {
		if raw, ok := backup.Data["releases"]; ok {
			var rels []*deployments.Release
			_ = json.Unmarshal(raw, &rels)
			wipeRepo(m.releaseRepo)
			for _, r := range rels {
				_ = m.releaseRepo.Create(ctx, r)
			}
		}
	}

	// 5. Wipe & Restore Events
	if m.eventRepo != nil {
		if raw, ok := backup.Data["events"]; ok {
			var evts []*deployments.Event
			_ = json.Unmarshal(raw, &evts)
			wipeRepo(m.eventRepo)
			for _, e := range evts {
				_ = m.eventRepo.Create(ctx, e)
			}
		}
	}

	// 6. Point-in-time recovery: Replay continuous WAL segments after base_lsn
	if targetTime != nil {
		replayedCount := 0
		for _, rec := range m.walLog {
			if rec.LSN > backup.BaseLSN && !rec.Timestamp.After(*targetTime) {
				m.replayRecord(ctx, rec)
				replayedCount++
			}
		}
		m.log.Info().
			Int("replayed_segments", replayedCount).
			Time("target_time", *targetTime).
			Msg("WAL continuous replay completed (PITR)")
	}

	return nil
}

func (m *MemoryBackupManager) replayRecord(ctx context.Context, rec *WALRecord) {
	switch rec.Table {
	case "deployments":
		var dep deployments.Deployment
		if err := json.Unmarshal(rec.Payload, &dep); err == nil {
			_ = m.depRepo.Create(ctx, &dep)
		}
	case "instances":
		var inst deployments.Instance
		if err := json.Unmarshal(rec.Payload, &inst); err == nil {
			_ = m.instRepo.Create(ctx, &inst)
		}
	case "projects":
		var proj projects.Project
		if err := json.Unmarshal(rec.Payload, &proj); err == nil {
			_ = m.projectRepo.Create(ctx, &proj)
		}
	case "events":
		var ev deployments.Event
		if err := json.Unmarshal(rec.Payload, &ev); err == nil {
			_ = m.eventRepo.Create(ctx, &ev)
		}
	}
}

// ListBackups returns all available base backups.
func (m *MemoryBackupManager) ListBackups(ctx context.Context) ([]*BaseBackup, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*BaseBackup
	for _, b := range m.backups {
		list = append(list, b)
	}
	return list, nil
}
