package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/secrets"
)

// PostgresSecretStore implements secrets.SecretStore backed by PostgreSQL.
// It guarantees that raw database rows contain ONLY encrypted ciphertext (SEC-01).
type PostgresSecretStore struct {
	pool        *pgxpool.Pool
	keyProvider secrets.KeyProvider
}

// NewPostgresSecretStore creates a new PostgreSQL-backed secret store.
func NewPostgresSecretStore(pool *pgxpool.Pool, keyProvider secrets.KeyProvider) *PostgresSecretStore {
	return &PostgresSecretStore{
		pool:        pool,
		keyProvider: keyProvider,
	}
}

// SetSecret encrypts plaintext using the active key version and stores ciphertext.
func (s *PostgresSecretStore) SetSecret(ctx context.Context, projectID, name, plaintext string) (*secrets.Secret, error) {
	keyVer := s.keyProvider.CurrentVersion()
	key, err := s.keyProvider.GetKey(keyVer)
	if err != nil {
		return nil, fmt.Errorf("get encryption key: %w", err)
	}

	ciphertext, err := secrets.Encrypt([]byte(plaintext), key)
	if err != nil {
		return nil, fmt.Errorf("encrypt secret: %w", err)
	}

	// Determine next version for this project + name
	var currentVer int
	queryVer := `SELECT COALESCE(MAX(version), 0) FROM secrets WHERE project_id = $1 AND name = $2 AND deployment_id IS NULL`
	if err := s.pool.QueryRow(ctx, queryVer, projectID, name).Scan(&currentVer); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("query secret version: %w", err)
	}
	nextVer := currentVer + 1

	now := time.Now().UTC()
	sec := &secrets.Secret{
		ID:         uuid.New().String(),
		ProjectID:  projectID,
		Name:       name,
		Ciphertext: ciphertext,
		KeyVersion: keyVer,
		Version:    nextVer,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	insertSQL := `
		INSERT INTO secrets (id, project_id, name, ciphertext, key_version, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = s.pool.Exec(ctx, insertSQL, sec.ID, sec.ProjectID, sec.Name, sec.Ciphertext, sec.KeyVersion, sec.Version, sec.CreatedAt, sec.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert secret: %w", err)
	}

	return sec, nil
}

// GetSecret retrieves and decrypts the latest version of a secret for a project.
func (s *PostgresSecretStore) GetSecret(ctx context.Context, projectID, name string) (*secrets.Secret, string, error) {
	query := `
		SELECT id, project_id, name, ciphertext, key_version, version, created_at, updated_at
		FROM secrets
		WHERE project_id = $1 AND name = $2 AND deployment_id IS NULL
		ORDER BY version DESC
		LIMIT 1
	`
	sec := &secrets.Secret{}
	err := s.pool.QueryRow(ctx, query, projectID, name).Scan(
		&sec.ID, &sec.ProjectID, &sec.Name, &sec.Ciphertext,
		&sec.KeyVersion, &sec.Version, &sec.CreatedAt, &sec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", secrets.ErrSecretNotFound
		}
		return nil, "", fmt.Errorf("query secret: %w", err)
	}

	key, err := s.keyProvider.GetKey(sec.KeyVersion)
	if err != nil {
		return nil, "", fmt.Errorf("get key version %d: %w", sec.KeyVersion, err)
	}

	pt, err := secrets.Decrypt(sec.Ciphertext, key)
	if err != nil {
		return nil, "", fmt.Errorf("decrypt secret %s: %w", name, err)
	}

	return sec, string(pt), nil
}

// GetSecretsForProject retrieves all current secrets for a project and decrypts them.
func (s *PostgresSecretStore) GetSecretsForProject(ctx context.Context, projectID string) (map[string]string, error) {
	query := `
		SELECT DISTINCT ON (name) name, ciphertext, key_version
		FROM secrets
		WHERE project_id = $1 AND deployment_id IS NULL
		ORDER BY name, version DESC
	`
	rows, err := s.pool.Query(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("query project secrets: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var name string
		var ciphertext []byte
		var keyVer int

		if err := rows.Scan(&name, &ciphertext, &keyVer); err != nil {
			return nil, fmt.Errorf("scan secret row: %w", err)
		}

		key, err := s.keyProvider.GetKey(keyVer)
		if err != nil {
			return nil, fmt.Errorf("get key version %d for %s: %w", keyVer, name, err)
		}

		pt, err := secrets.Decrypt(ciphertext, key)
		if err != nil {
			return nil, fmt.Errorf("decrypt secret %s: %w", name, err)
		}

		result[name] = string(pt)
	}

	return result, rows.Err()
}

// SnapshotForDeployment creates immutable deployment-scoped copies of active project secrets.
func (s *PostgresSecretStore) SnapshotForDeployment(ctx context.Context, projectID, deploymentID string) error {
	query := `
		INSERT INTO secrets (id, deployment_id, project_id, name, ciphertext, key_version, version, created_at, updated_at)
		SELECT gen_random_uuid(), $1, project_id, name, ciphertext, key_version, version, now(), now()
		FROM (
			SELECT DISTINCT ON (name) project_id, name, ciphertext, key_version, version
			FROM secrets
			WHERE project_id = $2 AND deployment_id IS NULL
			ORDER BY name, version DESC
		) active_secrets
	`
	_, err := s.pool.Exec(ctx, query, deploymentID, projectID)
	if err != nil {
		return fmt.Errorf("snapshot secrets for deployment %s: %w", deploymentID, err)
	}
	return nil
}

// GetSecretsForDeployment retrieves and decrypts the frozen secrets for a deployment.
func (s *PostgresSecretStore) GetSecretsForDeployment(ctx context.Context, deploymentID string) (map[string]string, error) {
	query := `
		SELECT name, ciphertext, key_version
		FROM secrets
		WHERE deployment_id = $1
	`
	rows, err := s.pool.Query(ctx, query, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("query deployment secrets: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var name string
		var ciphertext []byte
		var keyVer int

		if err := rows.Scan(&name, &ciphertext, &keyVer); err != nil {
			return nil, fmt.Errorf("scan deployment secret: %w", err)
		}

		key, err := s.keyProvider.GetKey(keyVer)
		if err != nil {
			return nil, fmt.Errorf("get key version %d for %s: %w", keyVer, name, err)
		}

		pt, err := secrets.Decrypt(ciphertext, key)
		if err != nil {
			return nil, fmt.Errorf("decrypt deployment secret %s: %w", name, err)
		}

		result[name] = string(pt)
	}

	return result, rows.Err()
}

// ListSecrets returns the metadata of secrets for a project without decrypting values.
func (s *PostgresSecretStore) ListSecrets(ctx context.Context, projectID string) ([]*secrets.Secret, error) {
	query := `
		SELECT DISTINCT ON (name) id, project_id, name, key_version, version, created_at, updated_at
		FROM secrets
		WHERE project_id = $1 AND deployment_id IS NULL
		ORDER BY name, version DESC
	`
	rows, err := s.pool.Query(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	defer rows.Close()

	var result []*secrets.Secret
	for rows.Next() {
		sec := &secrets.Secret{}
		if err := rows.Scan(&sec.ID, &sec.ProjectID, &sec.Name, &sec.KeyVersion, &sec.Version, &sec.CreatedAt, &sec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan secret: %w", err)
		}
		result = append(result, sec)
	}
	return result, rows.Err()
}
