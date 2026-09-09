package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/deployments"
)

// PostgresInstanceRepository implements deployments.InstanceRepository using PostgreSQL.
type PostgresInstanceRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresInstanceRepository(pool *pgxpool.Pool) *PostgresInstanceRepository {
	return &PostgresInstanceRepository{pool: pool}
}

func (r *PostgresInstanceRepository) Create(ctx context.Context, inst *deployments.Instance) error {
	query := `
		INSERT INTO instances (
			id, deployment_id, worker_id, instance_key, status, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, now(), now()
		)
		RETURNING created_at, updated_at;
	`

	var workerIDVal *string
	if inst.WorkerID != "" {
		workerIDVal = &inst.WorkerID
	}

	var createdAt, updatedAt time.Time
	err := r.pool.QueryRow(ctx, query,
		inst.ID,
		inst.DeploymentID,
		workerIDVal,
		inst.InstanceKey,
		inst.Status,
	).Scan(&createdAt, &updatedAt)

	if err != nil {
		return fmt.Errorf("failed to create instance: %w", err)
	}

	inst.CreatedAt = createdAt
	inst.UpdatedAt = updatedAt
	return nil
}

func (r *PostgresInstanceRepository) GetByID(ctx context.Context, id string) (*deployments.Instance, error) {
	query := `
		SELECT id, deployment_id, worker_id, instance_key, status, created_at, updated_at
		FROM instances
		WHERE id = $1;
	`
	return r.scanInstance(r.pool.QueryRow(ctx, query, id))
}

func (r *PostgresInstanceRepository) GetByInstanceKey(ctx context.Context, key string) (*deployments.Instance, error) {
	query := `
		SELECT id, deployment_id, worker_id, instance_key, status, created_at, updated_at
		FROM instances
		WHERE instance_key = $1;
	`
	return r.scanInstance(r.pool.QueryRow(ctx, query, key))
}

func (r *PostgresInstanceRepository) ListByDeployment(ctx context.Context, deploymentID string) ([]*deployments.Instance, error) {
	query := `
		SELECT id, deployment_id, worker_id, instance_key, status, created_at, updated_at
		FROM instances
		WHERE deployment_id = $1
		ORDER BY created_at ASC;
	`
	rows, err := r.pool.Query(ctx, query, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("failed to list instances: %w", err)
	}
	defer rows.Close()

	var result []*deployments.Instance
	for rows.Next() {
		inst, err := r.scanInstanceFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, inst)
	}

	return result, rows.Err()
}

func (r *PostgresInstanceRepository) ListByWorker(ctx context.Context, workerID string) ([]*deployments.Instance, error) {
	query := `
		SELECT id, deployment_id, worker_id, instance_key, status, created_at, updated_at
		FROM instances
		WHERE worker_id = $1
		ORDER BY created_at ASC;
	`
	rows, err := r.pool.Query(ctx, query, workerID)
	if err != nil {
		return nil, fmt.Errorf("failed to list instances by worker: %w", err)
	}
	defer rows.Close()

	var result []*deployments.Instance
	for rows.Next() {
		inst, err := r.scanInstanceFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, inst)
	}

	return result, rows.Err()
}

func (r *PostgresInstanceRepository) Update(ctx context.Context, inst *deployments.Instance) error {
	query := `
		UPDATE instances
		SET worker_id = $1, status = $2, updated_at = now()
		WHERE id = $3;
	`
	var workerIDVal *string
	if inst.WorkerID != "" {
		workerIDVal = &inst.WorkerID
	}

	tag, err := r.pool.Exec(ctx, query, workerIDVal, inst.Status, inst.ID)
	if err != nil {
		return fmt.Errorf("failed to update instance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return deployments.ErrInstanceNotFound
	}
	return nil
}

func (r *PostgresInstanceRepository) CountByWorkerForDeployment(ctx context.Context, deploymentID string) (map[string]int, error) {
	query := `
		SELECT worker_id, count(*)
		FROM instances
		WHERE deployment_id = $1 AND worker_id IS NOT NULL
		GROUP BY worker_id;
	`
	rows, err := r.pool.Query(ctx, query, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("failed to count instances by worker: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var workerID string
		var count int
		if err := rows.Scan(&workerID, &count); err != nil {
			return nil, err
		}
		counts[workerID] = count
	}

	return counts, rows.Err()
}

func (r *PostgresInstanceRepository) scanInstance(row pgx.Row) (*deployments.Instance, error) {
	var inst deployments.Instance
	var workerID sql.NullString

	err := row.Scan(
		&inst.ID,
		&inst.DeploymentID,
		&workerID,
		&inst.InstanceKey,
		&inst.Status,
		&inst.CreatedAt,
		&inst.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, deployments.ErrInstanceNotFound
		}
		return nil, fmt.Errorf("scan instance: %w", err)
	}

	if workerID.Valid {
		inst.WorkerID = workerID.String
	}
	return &inst, nil
}

func (r *PostgresInstanceRepository) scanInstanceFromRows(rows pgx.Rows) (*deployments.Instance, error) {
	var inst deployments.Instance
	var workerID sql.NullString

	err := rows.Scan(
		&inst.ID,
		&inst.DeploymentID,
		&workerID,
		&inst.InstanceKey,
		&inst.Status,
		&inst.CreatedAt,
		&inst.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan instance row: %w", err)
	}

	if workerID.Valid {
		inst.WorkerID = workerID.String
	}
	return &inst, nil
}

func (r *PostgresInstanceRepository) ListAll(ctx context.Context) ([]*deployments.Instance, error) {
	query := `
		SELECT id, deployment_id, worker_id, instance_key, status, created_at, updated_at
		FROM instances
		ORDER BY created_at ASC;
	`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all instances: %w", err)
	}
	defer rows.Close()

	var instances []*deployments.Instance
	for rows.Next() {
		inst, err := r.scanInstanceFromRows(rows)
		if err != nil {
			return nil, err
		}
		instances = append(instances, inst)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate instances: %w", err)
	}

	return instances, nil
}

var _ deployments.InstanceRepository = (*PostgresInstanceRepository)(nil)

