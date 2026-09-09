package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/workers"
)

// PostgresWorkerRepository implements workers.WorkerRepository using PostgreSQL.
type PostgresWorkerRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresWorkerRepository creates a new PostgresWorkerRepository.
func NewPostgresWorkerRepository(pool *pgxpool.Pool) *PostgresWorkerRepository {
	return &PostgresWorkerRepository{pool: pool}
}

func (r *PostgresWorkerRepository) Upsert(ctx context.Context, w *workers.Worker) error {
	labelsJSON, err := json.Marshal(w.Labels)
	if err != nil {
		labelsJSON = []byte("{}")
	}

	query := `
		INSERT INTO workers (
			worker_key, hostname, ip_address, capacity, labels, state, health, schedulable, last_beat_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, now()
		)
		ON CONFLICT (worker_key) DO UPDATE SET
			hostname = EXCLUDED.hostname,
			ip_address = EXCLUDED.ip_address,
			capacity = EXCLUDED.capacity,
			labels = EXCLUDED.labels,
			state = EXCLUDED.state,
			health = EXCLUDED.health,
			schedulable = EXCLUDED.schedulable,
			last_beat_at = EXCLUDED.last_beat_at,
			updated_at = now()
		RETURNING id, created_at, updated_at;
	`

	var id string
	var createdAt, updatedAt time.Time
	err = r.pool.QueryRow(ctx, query,
		w.WorkerKey,
		w.Hostname,
		w.IPAddress,
		w.Capacity,
		labelsJSON,
		string(w.State),
		string(w.Health),
		w.Schedulable,
		w.LastBeatAt,
	).Scan(&id, &createdAt, &updatedAt)

	if err != nil {
		return fmt.Errorf("failed to upsert worker: %w", err)
	}

	w.ID = id
	w.CreatedAt = createdAt
	w.UpdatedAt = updatedAt
	return nil
}

func (r *PostgresWorkerRepository) GetByID(ctx context.Context, id string) (*workers.Worker, error) {
	query := `
		SELECT id, worker_key, hostname, ip_address, capacity, labels, state, health, schedulable, last_beat_at, created_at, updated_at
		FROM workers
		WHERE id = $1;
	`
	return r.scanWorker(r.pool.QueryRow(ctx, query, id))
}

func (r *PostgresWorkerRepository) GetByWorkerKey(ctx context.Context, key string) (*workers.Worker, error) {
	query := `
		SELECT id, worker_key, hostname, ip_address, capacity, labels, state, health, schedulable, last_beat_at, created_at, updated_at
		FROM workers
		WHERE worker_key = $1;
	`
	return r.scanWorker(r.pool.QueryRow(ctx, query, key))
}

func (r *PostgresWorkerRepository) List(ctx context.Context) ([]*workers.Worker, error) {
	query := `
		SELECT id, worker_key, hostname, ip_address, capacity, labels, state, health, schedulable, last_beat_at, created_at, updated_at
		FROM workers
		ORDER BY created_at ASC;
	`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list workers: %w", err)
	}
	defer rows.Close()

	var result []*workers.Worker
	for rows.Next() {
		w, err := r.scanWorkerFromRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, w)
	}

	return result, rows.Err()
}

func (r *PostgresWorkerRepository) UpdateHeartbeat(ctx context.Context, workerKey string, beatAt time.Time) error {
	query := `
		UPDATE workers
		SET last_beat_at = $1, updated_at = now()
		WHERE worker_key = $2;
	`
	tag, err := r.pool.Exec(ctx, query, beatAt, workerKey)
	if err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return workers.ErrWorkerNotFound
	}
	return nil
}

func (r *PostgresWorkerRepository) SetDrain(ctx context.Context, workerKey string, draining bool) error {
	state := workers.StateReady
	schedulable := true
	if draining {
		state = workers.StateDraining
		schedulable = false
	}

	query := `
		UPDATE workers
		SET state = $1, schedulable = $2, updated_at = now()
		WHERE worker_key = $3;
	`
	tag, err := r.pool.Exec(ctx, query, string(state), schedulable, workerKey)
	if err != nil {
		return fmt.Errorf("failed to set drain status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return workers.ErrWorkerNotFound
	}
	return nil
}

func (r *PostgresWorkerRepository) UpdateActiveWorkloads(ctx context.Context, workerKey string, delta int) error {
	// In database, instances count is tracked directly via instances table;
	// this is a no-op or updates any cache column if needed.
	return nil
}

func (r *PostgresWorkerRepository) scanWorker(row pgx.Row) (*workers.Worker, error) {
	var w workers.Worker
	var labelsJSON []byte
	var stateStr, healthStr string

	err := row.Scan(
		&w.ID,
		&w.WorkerKey,
		&w.Hostname,
		&w.IPAddress,
		&w.Capacity,
		&labelsJSON,
		&stateStr,
		&healthStr,
		&w.Schedulable,
		&w.LastBeatAt,
		&w.CreatedAt,
		&w.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, workers.ErrWorkerNotFound
		}
		return nil, fmt.Errorf("scan worker: %w", err)
	}

	w.State = workers.WorkerState(stateStr)
	w.Health = workers.WorkerHealth(healthStr)
	if len(labelsJSON) > 0 {
		_ = json.Unmarshal(labelsJSON, &w.Labels)
	}
	if w.Labels == nil {
		w.Labels = make(map[string]string)
	}

	return &w, nil
}

func (r *PostgresWorkerRepository) scanWorkerFromRows(rows pgx.Rows) (*workers.Worker, error) {
	var w workers.Worker
	var labelsJSON []byte
	var stateStr, healthStr string

	err := rows.Scan(
		&w.ID,
		&w.WorkerKey,
		&w.Hostname,
		&w.IPAddress,
		&w.Capacity,
		&labelsJSON,
		&stateStr,
		&healthStr,
		&w.Schedulable,
		&w.LastBeatAt,
		&w.CreatedAt,
		&w.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan worker row: %w", err)
	}

	w.State = workers.WorkerState(stateStr)
	w.Health = workers.WorkerHealth(healthStr)
	if len(labelsJSON) > 0 {
		_ = json.Unmarshal(labelsJSON, &w.Labels)
	}
	if w.Labels == nil {
		w.Labels = make(map[string]string)
	}

	return &w, nil
}

var _ workers.WorkerRepository = (*PostgresWorkerRepository)(nil)
