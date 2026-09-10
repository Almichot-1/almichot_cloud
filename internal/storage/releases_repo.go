package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/deployments"
)

// Re-export core release types and errors from deployments domain.
type Release = deployments.Release
type ReleaseRepository = deployments.ReleaseRepository
type MemoryReleaseRepository = deployments.MemoryReleaseRepository

var (
	ErrReleaseNotFound         = deployments.ErrReleaseNotFound
	NewMemoryReleaseRepository = deployments.NewMemoryReleaseRepository
)

// PostgresReleaseRepository stores releases in PostgreSQL (§10.1, §19.1).
type PostgresReleaseRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresReleaseRepository creates a new PostgresReleaseRepository.
func NewPostgresReleaseRepository(pool *pgxpool.Pool) *PostgresReleaseRepository {
	return &PostgresReleaseRepository{pool: pool}
}

var _ deployments.ReleaseRepository = (*PostgresReleaseRepository)(nil)

func (r *PostgresReleaseRepository) Create(ctx context.Context, rel *deployments.Release) error {
	if rel.ID == "" {
		rel.ID = uuid.New().String()
	}
	if rel.CreatedAt.IsZero() {
		rel.CreatedAt = time.Now().UTC()
	}

	query := `
		INSERT INTO releases (id, project_id, deployment_id, version, image_ref, image_digest, signature, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.pool.Exec(ctx, query,
		rel.ID, rel.ProjectID, rel.DeploymentID, rel.Version, rel.ImageRef, rel.ImageDigest, rel.Signature, rel.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert release: %w", err)
	}
	return nil
}

func (r *PostgresReleaseRepository) Get(ctx context.Context, id string) (*deployments.Release, error) {
	query := `
		SELECT id, project_id, deployment_id, version, image_ref, image_digest, signature, created_at
		FROM releases WHERE id = $1
	`
	var rel deployments.Release
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&rel.ID, &rel.ProjectID, &rel.DeploymentID, &rel.Version, &rel.ImageRef, &rel.ImageDigest, &rel.Signature, &rel.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrReleaseNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query release by id: %w", err)
	}
	return &rel, nil
}

func (r *PostgresReleaseRepository) GetByDigest(ctx context.Context, digest string) (*deployments.Release, error) {
	query := `
		SELECT id, project_id, deployment_id, version, image_ref, image_digest, signature, created_at
		FROM releases WHERE image_digest = $1
		ORDER BY created_at DESC LIMIT 1
	`
	var rel deployments.Release
	err := r.pool.QueryRow(ctx, query, digest).Scan(
		&rel.ID, &rel.ProjectID, &rel.DeploymentID, &rel.Version, &rel.ImageRef, &rel.ImageDigest, &rel.Signature, &rel.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrReleaseNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query release by digest: %w", err)
	}
	return &rel, nil
}

func (r *PostgresReleaseRepository) ListByProject(ctx context.Context, projectID string) ([]*deployments.Release, error) {
	query := `
		SELECT id, project_id, deployment_id, version, image_ref, image_digest, signature, created_at
		FROM releases WHERE project_id = $1
		ORDER BY created_at DESC
	`
	rows, err := r.pool.Query(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("query releases by project: %w", err)
	}
	defer rows.Close()

	var releases []*deployments.Release
	for rows.Next() {
		var rel deployments.Release
		if err := rows.Scan(&rel.ID, &rel.ProjectID, &rel.DeploymentID, &rel.Version, &rel.ImageRef, &rel.ImageDigest, &rel.Signature, &rel.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan release: %w", err)
		}
		releases = append(releases, &rel)
	}
	return releases, rows.Err()
}
