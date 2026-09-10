package backup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// PostgresBackupManager implements BackupManager using real pg_dump / pg_restore binaries.
// It issues subprocess calls with real file I/O, proving the actual backup toolchain works —
// unlike MemoryBackupManager which only copies Go structs in the same process.
//
// Used exclusively by G-46 real-infra gate. Requires pg_dump and pg_restore binaries
// on PATH (postgresql-client package). If either binary is missing, callers should
// skip the test with t.Skip.
type PostgresBackupManager struct {
	// srcDSN is the source Postgres connection string (data is backed up from here).
	srcDSN string
	// dstDSN is the destination Postgres connection string (data is restored into here).
	// Must point to a DIFFERENT Postgres instance/database from srcDSN.
	dstDSN string
	// dumpDir is the directory where dump files are written.
	dumpDir string
	// dstPool is an open connection to the destination database (for post-restore verification).
	dstPool *pgxpool.Pool
	log     zerolog.Logger
}

// PostgresBackupResult records the paths and metadata of a completed backup.
type PostgresBackupResult struct {
	// ID is a unique identifier for this backup.
	ID string
	// DumpFilePath is the absolute path to the pg_dump custom-format file.
	DumpFilePath string
	// CreatedAt is when the backup was taken.
	CreatedAt time.Time
	// SizeBytesApprox is the on-disk size of the dump file.
	SizeBytesApprox int64
}

// NewPostgresBackupManager creates a backup manager backed by real pg_dump/pg_restore.
// srcDSN and dstDSN must be valid Postgres DSNs for SOURCE and DESTINATION respectively.
// The caller is responsible for ensuring the destination DB is fresh (no schema applied).
func NewPostgresBackupManager(srcDSN, dstDSN, dumpDir string, log zerolog.Logger) (*PostgresBackupManager, error) {
	if _, err := exec.LookPath("pg_dump"); err != nil {
		return nil, fmt.Errorf("pg_dump binary not found on PATH: %w", err)
	}
	if _, err := exec.LookPath("pg_restore"); err != nil {
		return nil, fmt.Errorf("pg_restore binary not found on PATH: %w", err)
	}

	if err := os.MkdirAll(dumpDir, 0o700); err != nil {
		return nil, fmt.Errorf("create dump dir %s: %w", dumpDir, err)
	}

	return &PostgresBackupManager{
		srcDSN:  srcDSN,
		dstDSN:  dstDSN,
		dumpDir: dumpDir,
		log:     log.With().Str("component", "postgres-backup-manager").Logger(),
	}, nil
}

// OpenDstPool opens a pgxpool.Pool against the destination database and stores it
// for post-restore verification queries. Must be called after Restore.
func (m *PostgresBackupManager) OpenDstPool(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, m.dstDSN)
	if err != nil {
		return nil, fmt.Errorf("open dst pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping dst pool: %w", err)
	}
	m.dstPool = pool
	return pool, nil
}

// CreateBackup shells out to pg_dump, producing a real custom-format dump file on disk.
// The dump is of srcDSN. Returns metadata about the dump file.
//
// This method makes real subprocess calls and real file I/O — it cannot be satisfied
// by an in-process mock.
func (m *PostgresBackupManager) CreateBackup(ctx context.Context) (*PostgresBackupResult, error) {
	backupID := fmt.Sprintf("backup-%d", time.Now().UnixNano())
	dumpPath := fmt.Sprintf("%s/%s.dump", m.dumpDir, backupID)

	m.log.Info().
		Str("backup_id", backupID).
		Str("dump_path", dumpPath).
		Str("src", sanitizeDSN(m.srcDSN)).
		Msg("starting pg_dump (real subprocess)")

	// pg_dump --format=custom produces a binary format suitable for pg_restore --list / --section
	//nolint:gosec // DSN contains credentials, but this is a test helper only.
	cmd := exec.CommandContext(ctx,
		"pg_dump",
		"--format=custom",
		"--no-password",
		fmt.Sprintf("--file=%s", dumpPath),
		m.srcDSN,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("pg_dump failed: %w", err)
	}

	fi, err := os.Stat(dumpPath)
	if err != nil {
		return nil, fmt.Errorf("stat dump file: %w", err)
	}

	m.log.Info().
		Str("backup_id", backupID).
		Int64("size_bytes", fi.Size()).
		Msg("pg_dump completed successfully")

	return &PostgresBackupResult{
		ID:              backupID,
		DumpFilePath:    dumpPath,
		CreatedAt:       time.Now().UTC(),
		SizeBytesApprox: fi.Size(),
	}, nil
}

// RestoreDump shells out to pg_restore, loading the dump file into dstDSN.
// The destination database must already exist (but schema/data need not).
// After RestoreDump, use OpenDstPool + QueryDst to verify the restored data.
//
// This method makes real subprocess calls and real network connections — it cannot be
// satisfied by an in-process mock.
func (m *PostgresBackupManager) RestoreDump(ctx context.Context, backup *PostgresBackupResult) error {
	if backup == nil {
		return fmt.Errorf("cannot restore from nil backup result")
	}

	m.log.Warn().
		Str("backup_id", backup.ID).
		Str("dump_path", backup.DumpFilePath).
		Str("dst", sanitizeDSN(m.dstDSN)).
		Msg("starting pg_restore into destination database (real subprocess)")

	//nolint:gosec // DSN contains credentials, but this is a test helper only.
	cmd := exec.CommandContext(ctx,
		"pg_restore",
		"--clean",
		"--if-exists",
		"--no-password",
		fmt.Sprintf("--dbname=%s", m.dstDSN),
		backup.DumpFilePath,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_restore failed: %w", err)
	}

	m.log.Info().
		Str("backup_id", backup.ID).
		Msg("pg_restore completed successfully")

	return nil
}

// QueryDst executes a SQL query against the destination database (after Restore).
// Used by G-46 to verify that pre-backup data IS present and post-backup-drift data is NOT.
func (m *PostgresBackupManager) QueryDst(ctx context.Context, sql string, args ...any) (int64, error) {
	if m.dstPool == nil {
		return 0, fmt.Errorf("dstPool not open; call OpenDstPool first")
	}
	var count int64
	err := m.dstPool.QueryRow(ctx, sql, args...).Scan(&count)
	return count, err
}

// sanitizeDSN redacts the password from a Postgres DSN for logging.
func sanitizeDSN(dsn string) string {
	// Simple redaction: replace password=... or :password@ patterns
	if idx := strings.Index(dsn, "@"); idx != -1 {
		prefix := dsn[:idx]
		if colIdx := strings.LastIndex(prefix, ":"); colIdx != -1 {
			return prefix[:colIdx] + ":***@" + dsn[idx+1:]
		}
	}
	return "<dsn-redacted>"
}

// CreateBaseBackup, ArchiveWAL, ListBackups implement the BackupManager interface for
// compatibility with existing code that uses BackupManager. The real gate test uses the
// richer PostgresBackupResult-returning methods above.
func (m *PostgresBackupManager) CreateBaseBackup(ctx context.Context) (*BaseBackup, error) {
	result, err := m.CreateBackup(ctx)
	if err != nil {
		return nil, err
	}
	// Wrap in BaseBackup so the interface is satisfied; DumpFilePath is stored in ID.
	return &BaseBackup{
		ID:        result.ID,
		CreatedAt: result.CreatedAt,
		Data:      map[string][]byte{"dump_path": []byte(result.DumpFilePath)},
	}, nil
}

func (m *PostgresBackupManager) ArchiveWAL(_ context.Context, _ *WALRecord) error {
	// Postgres manages WAL internally; this is a no-op for the Postgres-backed manager.
	return nil
}

func (m *PostgresBackupManager) Restore(ctx context.Context, backup *BaseBackup, _ *time.Time) error {
	if backup == nil {
		return fmt.Errorf("cannot restore from nil backup")
	}
	dumpPath := string(backup.Data["dump_path"])
	return m.RestoreDump(ctx, &PostgresBackupResult{
		ID:           backup.ID,
		DumpFilePath: dumpPath,
		CreatedAt:    backup.CreatedAt,
	})
}

func (m *PostgresBackupManager) ListBackups(_ context.Context) ([]*BaseBackup, error) {
	return nil, nil // not needed for gate tests
}
