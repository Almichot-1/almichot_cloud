package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/deployments"
)

// Re-export core event types from deployments domain.
type Event = deployments.Event
type EventRepository = deployments.EventRepository
type MemoryEventRepository = deployments.MemoryEventRepository

var NewMemoryEventRepository = deployments.NewMemoryEventRepository

// PostgresEventRepository stores operational events in PostgreSQL (§19.1, §22, §27.2).
type PostgresEventRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresEventRepository creates a new PostgresEventRepository.
func NewPostgresEventRepository(pool *pgxpool.Pool) *PostgresEventRepository {
	return &PostgresEventRepository{pool: pool}
}

var _ deployments.EventRepository = (*PostgresEventRepository)(nil)

func (r *PostgresEventRepository) Create(ctx context.Context, ev *deployments.Event) error {
	if ev.ID == "" {
		ev.ID = uuid.New().String()
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}
	metaBytes, err := json.Marshal(ev.Metadata)
	if err != nil {
		metaBytes = []byte("{}")
	}

	query := `
		INSERT INTO events (id, project_id, deployment_id, event_type, message, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err = r.pool.Exec(ctx, query,
		ev.ID, ev.ProjectID, ev.DeploymentID, ev.EventType, ev.Message, metaBytes, ev.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

func (r *PostgresEventRepository) ListByProject(ctx context.Context, projectID string) ([]*deployments.Event, error) {
	query := `
		SELECT id, project_id, deployment_id, event_type, message, metadata, created_at
		FROM events WHERE project_id = $1
		ORDER BY created_at DESC
	`
	rows, err := r.pool.Query(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("query events by project: %w", err)
	}
	defer rows.Close()

	var result []*deployments.Event
	for rows.Next() {
		var ev deployments.Event
		var metaBytes []byte
		if err := rows.Scan(&ev.ID, &ev.ProjectID, &ev.DeploymentID, &ev.EventType, &ev.Message, &metaBytes, &ev.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		if len(metaBytes) > 0 {
			_ = json.Unmarshal(metaBytes, &ev.Metadata)
		}
		result = append(result, &ev)
	}
	return result, rows.Err()
}

func (r *PostgresEventRepository) ListByDeployment(ctx context.Context, deploymentID string) ([]*deployments.Event, error) {
	query := `
		SELECT id, project_id, deployment_id, event_type, message, metadata, created_at
		FROM events WHERE deployment_id = $1
		ORDER BY created_at DESC
	`
	rows, err := r.pool.Query(ctx, query, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("query events by deployment: %w", err)
	}
	defer rows.Close()

	var result []*deployments.Event
	for rows.Next() {
		var ev deployments.Event
		var metaBytes []byte
		if err := rows.Scan(&ev.ID, &ev.ProjectID, &ev.DeploymentID, &ev.EventType, &ev.Message, &metaBytes, &ev.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		if len(metaBytes) > 0 {
			_ = json.Unmarshal(metaBytes, &ev.Metadata)
		}
		result = append(result, &ev)
	}
	return result, rows.Err()
}
