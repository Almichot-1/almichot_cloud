package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrSecretNotFound is returned when a requested secret does not exist.
	ErrSecretNotFound = errors.New("secret not found")
	// ErrCiphertextTooShort is returned when attempting to decrypt invalid ciphertext.
	ErrCiphertextTooShort = errors.New("ciphertext too short: missing nonce")
)

// Secret represents an envelope-encrypted secret record.
type Secret struct {
	ID           string    `json:"id"`
	ProjectID    string    `json:"project_id"`
	DeploymentID string    `json:"deployment_id,omitempty"`
	Name         string    `json:"name"`
	Ciphertext   []byte    `json:"-"` // Never exposed in JSON serialization
	KeyVersion   int       `json:"key_version"`
	Version      int       `json:"version"` // Logical version of the secret (increments on rotation)
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Encrypt encrypts plaintext using AES-256-GCM with a randomized 12-byte nonce.
// Returns nonce prepended to the ciphertext and auth tag.
func Encrypt(plaintext []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("read random nonce: %w", err)
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts AES-256-GCM ciphertext by extracting the 12-byte nonce prefix.
func Decrypt(ciphertext []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, ErrCiphertextTooShort
	}

	nonce, encrypted := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, encrypted, nil)
}

// SecretStore defines persistent operations for encrypted secrets.
type SecretStore interface {
	SetSecret(ctx context.Context, projectID, name, plaintext string) (*Secret, error)
	GetSecret(ctx context.Context, projectID, name string) (*Secret, string, error)
	GetSecretsForProject(ctx context.Context, projectID string) (map[string]string, error)
	GetSecretsForDeployment(ctx context.Context, deploymentID string) (map[string]string, error)
	SnapshotForDeployment(ctx context.Context, projectID, deploymentID string) error
	ListSecrets(ctx context.Context, projectID string) ([]*Secret, error)
}

// MemorySecretStore is an in-memory implementation of SecretStore.
// It stores ONLY encrypted ciphertext, never plaintext.
type MemorySecretStore struct {
	mu          sync.RWMutex
	keyProvider KeyProvider
	// Keyed by projectID + ":" + name -> latest *Secret
	projectSecrets map[string]*Secret
	// Keyed by deploymentID + ":" + name -> snapshot *Secret
	deploymentSecrets map[string]*Secret
}

// NewMemorySecretStore creates a new in-memory secret store.
func NewMemorySecretStore(keyProvider KeyProvider) *MemorySecretStore {
	return &MemorySecretStore{
		keyProvider:       keyProvider,
		projectSecrets:    make(map[string]*Secret),
		deploymentSecrets: make(map[string]*Secret),
	}
}

// SetSecret encrypts and stores a secret for a project at version 1 (or increments if existing).
func (s *MemorySecretStore) SetSecret(ctx context.Context, projectID, name, plaintext string) (*Secret, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyVer := s.keyProvider.CurrentVersion()
	key, err := s.keyProvider.GetKey(keyVer)
	if err != nil {
		return nil, fmt.Errorf("get encryption key: %w", err)
	}

	ciphertext, err := Encrypt([]byte(plaintext), key)
	if err != nil {
		return nil, fmt.Errorf("encrypt secret: %w", err)
	}

	mapKey := projectID + ":" + name
	version := 1
	if existing, ok := s.projectSecrets[mapKey]; ok {
		version = existing.Version + 1
	}

	now := time.Now().UTC()
	sec := &Secret{
		ID:         uuid.New().String(),
		ProjectID:  projectID,
		Name:       name,
		Ciphertext: ciphertext,
		KeyVersion: keyVer,
		Version:    version,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	s.projectSecrets[mapKey] = sec
	return sec, nil
}

// GetSecret retrieves a secret and decrypts its plaintext value in memory.
func (s *MemorySecretStore) GetSecret(ctx context.Context, projectID, name string) (*Secret, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	mapKey := projectID + ":" + name
	sec, ok := s.projectSecrets[mapKey]
	if !ok {
		return nil, "", ErrSecretNotFound
	}

	key, err := s.keyProvider.GetKey(sec.KeyVersion)
	if err != nil {
		return nil, "", fmt.Errorf("get key version %d: %w", sec.KeyVersion, err)
	}

	plaintextBytes, err := Decrypt(sec.Ciphertext, key)
	if err != nil {
		return nil, "", fmt.Errorf("decrypt secret: %w", err)
	}

	return sec, string(plaintextBytes), nil
}

// GetSecretsForProject retrieves and decrypts all current secrets for a project.
func (s *MemorySecretStore) GetSecretsForProject(ctx context.Context, projectID string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string)
	for mapKey, sec := range s.projectSecrets {
		if sec.ProjectID == projectID {
			key, err := s.keyProvider.GetKey(sec.KeyVersion)
			if err != nil {
				return nil, fmt.Errorf("get key version %d for %s: %w", sec.KeyVersion, mapKey, err)
			}
			pt, err := Decrypt(sec.Ciphertext, key)
			if err != nil {
				return nil, fmt.Errorf("decrypt secret %s: %w", sec.Name, err)
			}
			result[sec.Name] = string(pt)
		}
	}
	return result, nil
}

// SnapshotForDeployment freezes current project secrets into a deployment-scoped snapshot.
func (s *MemorySecretStore) SnapshotForDeployment(ctx context.Context, projectID, deploymentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	for _, sec := range s.projectSecrets {
		if sec.ProjectID == projectID {
			// Deep copy ciphertext
			cpCiphertext := make([]byte, len(sec.Ciphertext))
			copy(cpCiphertext, sec.Ciphertext)

			snap := &Secret{
				ID:           uuid.New().String(),
				ProjectID:    projectID,
				DeploymentID: deploymentID,
				Name:         sec.Name,
				Ciphertext:   cpCiphertext,
				KeyVersion:   sec.KeyVersion,
				Version:      sec.Version,
				CreatedAt:    now,
				UpdatedAt:    now,
			}
			s.deploymentSecrets[deploymentID+":"+sec.Name] = snap
		}
	}
	return nil
}

// GetSecretsForDeployment retrieves and decrypts the frozen secrets for a specific deployment.
func (s *MemorySecretStore) GetSecretsForDeployment(ctx context.Context, deploymentID string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string)
	for _, sec := range s.deploymentSecrets {
		if sec.DeploymentID == deploymentID {
			key, err := s.keyProvider.GetKey(sec.KeyVersion)
			if err != nil {
				return nil, fmt.Errorf("get key version %d: %w", sec.KeyVersion, err)
			}
			pt, err := Decrypt(sec.Ciphertext, key)
			if err != nil {
				return nil, fmt.Errorf("decrypt deployment secret %s: %w", sec.Name, err)
			}
			result[sec.Name] = string(pt)
		}
	}
	return result, nil
}

// ListSecrets returns the metadata of secrets for a project without decrypting values.
func (s *MemorySecretStore) ListSecrets(ctx context.Context, projectID string) ([]*Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*Secret
	for _, sec := range s.projectSecrets {
		if sec.ProjectID == projectID {
			clone := *sec
			clone.Ciphertext = nil // Redact ciphertext in listing
			result = append(result, &clone)
		}
	}
	return result, nil
}
