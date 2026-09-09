package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/deployments"
)

// PostgresDeploymentRepository implements deployments.DeploymentRepository using PostgreSQL.
type PostgresDeploymentRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresDeploymentRepository(pool *pgxpool.Pool) *PostgresDeploymentRepository {
	return &PostgresDeploymentRepository{pool: pool}
}

func (r *PostgresDeploymentRepository) Create(ctx context.Context, d *deployments.Deployment) error {
	envJSON, err := json.Marshal(d.Env)
	if err != nil {
		envJSON = []byte("{}")
	}

	query := `
		INSERT INTO deployments (
			id, project_id, revision, image, image_digest, desired_state, status, stage, instance_count, env, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), now()
		)
		RETURNING created_at, updated_at;
	`

	var createdAt, updatedAt time.Time
	err = r.pool.QueryRow(ctx, query,
		d.ID,
		d.ProjectID,
		d.Revision,
		d.Image,
		d.ImageDigest,
		d.DesiredState,
		string(d.Status),
		d.Stage,
		d.InstanceCount,
		envJSON,
	).Scan(&createdAt, &updatedAt)

	if err != nil {
		return fmt.Errorf("failed to create deployment: %w", err)
	}

	d.CreatedAt = createdAt
	d.UpdatedAt = updatedAt
	return nil
}

func (r *PostgresDeploymentRepository) GetByID(ctx context.Context, id string) (*deployments.Deployment, error) {
	query := `
		SELECT id, project_id, revision, image, image_digest, desired_state, status, stage, instance_count, env, created_at, updated_at
		FROM deployments
		WHERE id = $1;
	`
	var d deployments.Deployment
	var envJSON []byte
	var statusStr string

	err := r.pool.QueryRow(ctx, query, id).Scan(
		&d.ID,
		&d.ProjectID,
		&d.Revision,
		&d.Image,
		&d.ImageDigest,
		&d.DesiredState,
		&statusStr,
		&d.Stage,
		&d.InstanceCount,
		&envJSON,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, deployments.ErrDeploymentNotFound
		}
		return nil, fmt.Errorf("scan deployment: %w", err)
	}

	d.Status = deployments.DeploymentStatus(statusStr)
	if len(envJSON) > 0 {
		_ = json.Unmarshal(envJSON, &d.Env)
	}
	if d.Env == nil {
		d.Env = make(map[string]string)
	}

	return &d, nil
}

func (r *PostgresDeploymentRepository) UpdateStatus(ctx context.Context, id string, status deployments.DeploymentStatus, stage string) error {
	query := `
		UPDATE deployments
		SET status = $1, stage = $2, updated_at = now()
		WHERE id = $3;
	`
	tag, err := r.pool.Exec(ctx, query, string(status), stage, id)
	if err != nil {
		return fmt.Errorf("failed to update deployment status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return deployments.ErrDeploymentNotFound
	}
	return nil
}

func (r *PostgresDeploymentRepository) UpdateImage(ctx context.Context, id string, image string, imageDigest string) error {
	query := `
		UPDATE deployments
		SET image = $1, image_digest = $2, updated_at = now()
		WHERE id = $3;
	`
	tag, err := r.pool.Exec(ctx, query, image, imageDigest, id)
	if err != nil {
		return fmt.Errorf("failed to update deployment image: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return deployments.ErrDeploymentNotFound
	}
	return nil
}

func (r *PostgresDeploymentRepository) List(ctx context.Context, projectID string) ([]*deployments.Deployment, error) {
	query := `
		SELECT id, project_id, revision, image, image_digest, desired_state, status, stage, instance_count, env, created_at, updated_at
		FROM deployments
		WHERE ($1 = '' OR project_id = $1::uuid)
		ORDER BY created_at DESC;
	`
	rows, err := r.pool.Query(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to list deployments: %w", err)
	}
	defer rows.Close()

	var result []*deployments.Deployment
	for rows.Next() {
		var d deployments.Deployment
		var envJSON []byte
		var statusStr string

		err := rows.Scan(
			&d.ID,
			&d.ProjectID,
			&d.Revision,
			&d.Image,
			&d.ImageDigest,
			&d.DesiredState,
			&statusStr,
			&d.Stage,
			&d.InstanceCount,
			&envJSON,
			&d.CreatedAt,
			&d.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		d.Status = deployments.DeploymentStatus(statusStr)
		if len(envJSON) > 0 {
			_ = json.Unmarshal(envJSON, &d.Env)
		}
		result = append(result, &d)
	}

	return result, rows.Err()
}

var _ deployments.DeploymentRepository = (*PostgresDeploymentRepository)(nil)
