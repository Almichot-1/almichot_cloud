package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/projects"
)

// PostgresProjectRepository implements projects.ProjectRepository using PostgreSQL.
type PostgresProjectRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresProjectRepository creates a new PostgresProjectRepository.
func NewPostgresProjectRepository(pool *pgxpool.Pool) *PostgresProjectRepository {
	return &PostgresProjectRepository{pool: pool}
}

func (r *PostgresProjectRepository) Create(ctx context.Context, p *projects.Project) error {
	query := `
		INSERT INTO projects (name, description, repo_url, webhook_secret)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO NOTHING
		RETURNING id, created_at, updated_at;
	`

	var id string
	var createdAt, updatedAt time.Time
	err := r.pool.QueryRow(ctx, query, p.Name, p.Description, p.RepoURL, p.WebhookSecret).Scan(&id, &createdAt, &updatedAt)
	if err != nil {
		return fmt.Errorf("failed to create project: %w", err)
	}

	p.ID = id
	p.CreatedAt = createdAt
	p.UpdatedAt = updatedAt
	return nil
}

func (r *PostgresProjectRepository) GetByID(ctx context.Context, id string) (*projects.Project, error) {
	query := `
		SELECT id, name, description, repo_url, COALESCE(webhook_secret, ''), created_at, updated_at
		FROM projects
		WHERE id = $1;
	`
	return r.scanProject(r.pool.QueryRow(ctx, query, id))
}

func (r *PostgresProjectRepository) GetByName(ctx context.Context, name string) (*projects.Project, error) {
	query := `
		SELECT id, name, description, repo_url, COALESCE(webhook_secret, ''), created_at, updated_at
		FROM projects
		WHERE name = $1;
	`
	return r.scanProject(r.pool.QueryRow(ctx, query, name))
}

func (r *PostgresProjectRepository) List(ctx context.Context) ([]*projects.Project, error) {
	query := `
		SELECT id, name, description, repo_url, COALESCE(webhook_secret, ''), created_at, updated_at
		FROM projects
		ORDER BY created_at ASC;
	`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list projects: %w", err)
	}
	defer rows.Close()

	var result []*projects.Project
	for rows.Next() {
		var p projects.Project
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.RepoURL, &p.WebhookSecret, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, &p)
	}
	return result, rows.Err()
}

func (r *PostgresProjectRepository) scanProject(row pgx.Row) (*projects.Project, error) {
	var p projects.Project
	err := row.Scan(&p.ID, &p.Name, &p.Description, &p.RepoURL, &p.WebhookSecret, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, projects.ErrProjectNotFound
		}
		return nil, fmt.Errorf("scan project: %w", err)
	}
	return &p, nil
}

var _ projects.ProjectRepository = (*PostgresProjectRepository)(nil)