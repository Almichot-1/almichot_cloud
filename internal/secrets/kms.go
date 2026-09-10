package secrets

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

var (
	// ErrKMSUnavailable is returned when KMS or Vault is unreachable.
	ErrKMSUnavailable = errors.New("KMS/Vault service is unavailable")
	// ErrInvalidMasterKeyID is returned when an unrecognized key ID is provided.
	ErrInvalidMasterKeyID = errors.New("unknown or invalid KMS master key ID")
)

// KMSClient abstracts envelope key wrapping operations against AWS KMS, HashiCorp Vault, or GCP KMS.
type KMSClient interface {
	WrapKey(ctx context.Context, keyID string, plaintextDEK []byte) (wrappedDEK []byte, err error)
	UnwrapKey(ctx context.Context, keyID string, wrappedDEK []byte) (plaintextDEK []byte, err error)
	RotateKey(ctx context.Context) (newKeyID string, err error)
	ReWrapKey(ctx context.Context, oldKeyID, newKeyID string, wrappedDEK []byte) (newWrappedDEK []byte, err error)
	CurrentKeyID() string
	SetAvailable(available bool)
}

// MockKMSClient implements a high-fidelity in-memory KMS/Vault service.
// The master key material is confined strictly inside the KMS struct boundary and is
// NEVER placed in the process environment or written to config files at rest (§21.2, G-36).
type MockKMSClient struct {
	mu           sync.RWMutex
	currentKeyID string
	keys         map[string][]byte
	available    bool
}

// NewMockKMSClient creates a new KMS/Vault client initialized with an initial master key.
func NewMockKMSClient() *MockKMSClient {
	initialKey := make([]byte, 32)
	_, _ = io.ReadFull(rand.Reader, initialKey)
	keyID := "nebula-kms-master-v1"

	return &MockKMSClient{
		currentKeyID: keyID,
		keys: map[string][]byte{
			keyID: initialKey,
		},
		available: true,
	}
}

// SetAvailable configures whether the KMS responds to requests (used for failure-mode testing).
func (m *MockKMSClient) SetAvailable(avail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.available = avail
}

// CurrentKeyID returns the currently active master key ID for wrapping new data keys.
func (m *MockKMSClient) CurrentKeyID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentKeyID
}

// WrapKey encrypts a plaintext DEK using the specified master key.
func (m *MockKMSClient) WrapKey(ctx context.Context, keyID string, plaintextDEK []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.available {
		return nil, ErrKMSUnavailable
	}

	masterKey, ok := m.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidMasterKeyID, keyID)
	}

	return Encrypt(plaintextDEK, masterKey)
}

// UnwrapKey decrypts a wrapped DEK using the specified master key.
func (m *MockKMSClient) UnwrapKey(ctx context.Context, keyID string, wrappedDEK []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.available {
		return nil, ErrKMSUnavailable
	}

	masterKey, ok := m.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidMasterKeyID, keyID)
	}

	return Decrypt(wrappedDEK, masterKey)
}

// RotateKey generates a new master key version inside the KMS boundary and marks it current.
func (m *MockKMSClient) RotateKey(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.available {
		return "", ErrKMSUnavailable
	}

	newKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, newKey); err != nil {
		return "", fmt.Errorf("generate new master key: %w", err)
	}

	nextVersion := len(m.keys) + 1
	newKeyID := fmt.Sprintf("nebula-kms-master-v%d", nextVersion)
	m.keys[newKeyID] = newKey
	m.currentKeyID = newKeyID

	return newKeyID, nil
}

// ReWrapKey decrypts a wrapped DEK with oldKeyID and re-encrypts it with newKeyID atomically.
func (m *MockKMSClient) ReWrapKey(ctx context.Context, oldKeyID, newKeyID string, wrappedDEK []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.available {
		return nil, ErrKMSUnavailable
	}

	oldMasterKey, ok := m.keys[oldKeyID]
	if !ok {
		return nil, fmt.Errorf("%w: old key %s", ErrInvalidMasterKeyID, oldKeyID)
	}
	newMasterKey, ok := m.keys[newKeyID]
	if !ok {
		return nil, fmt.Errorf("%w: new key %s", ErrInvalidMasterKeyID, newKeyID)
	}

	plaintextDEK, err := Decrypt(wrappedDEK, oldMasterKey)
	if err != nil {
		return nil, fmt.Errorf("unwrap with old master key: %w", err)
	}

	return Encrypt(plaintextDEK, newMasterKey)
}

// KMSEnvelopeKeyProvider manages versioned DEKs wrapped by a KMS/Vault (§21.2, Phase 13).
// Stored state contains ONLY wrapped ciphertext, never the plaintext master key.
type KMSEnvelopeKeyProvider struct {
	mu             sync.RWMutex
	kms            KMSClient
	currentVersion int
	// keyVersion -> wrapped DEK
	wrappedDEKs map[int][]byte
	// keyVersion -> KMS master key ID that wrapped it
	keyIDs map[int]string
}

// NewKMSEnvelopeKeyProvider creates a key provider using KMS for envelope key wrapping.
func NewKMSEnvelopeKeyProvider(kms KMSClient) (*KMSEnvelopeKeyProvider, error) {
	if kms == nil {
		return nil, errors.New("KMS client cannot be nil")
	}

	provider := &KMSEnvelopeKeyProvider{
		kms:            kms,
		currentVersion: 1,
		wrappedDEKs:    make(map[int][]byte),
		keyIDs:         make(map[int]string),
	}

	// Initialize version 1 DEK
	_, err := provider.GenerateAndWrapKey(context.Background(), 1)
	if err != nil {
		return nil, fmt.Errorf("initialize DEK v1: %w", err)
	}

	return provider, nil
}

// GenerateAndWrapKey generates a new random 32-byte DEK, wraps it with KMS, and saves the ciphertext.
func (p *KMSEnvelopeKeyProvider) GenerateAndWrapKey(ctx context.Context, version int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, fmt.Errorf("generate random DEK: %w", err)
	}

	keyID := p.kms.CurrentKeyID()
	wrapped, err := p.kms.WrapKey(ctx, keyID, dek)
	if err != nil {
		return nil, fmt.Errorf("wrap DEK with KMS: %w", err)
	}

	p.wrappedDEKs[version] = wrapped
	p.keyIDs[version] = keyID
	if version >= p.currentVersion {
		p.currentVersion = version
	}

	return dek, nil
}

// GetKey unwraps the versioned DEK from KMS transiently into memory.
func (p *KMSEnvelopeKeyProvider) GetKey(version int) ([]byte, error) {
	p.mu.RLock()
	wrapped, ok := p.wrappedDEKs[version]
	keyID, okKeyID := p.keyIDs[version]
	p.mu.RUnlock()

	if !ok || !okKeyID {
		return nil, fmt.Errorf("%w: %d", ErrKeyNotFound, version)
	}

	// Unwrap DEK via KMS
	plaintextDEK, err := p.kms.UnwrapKey(context.Background(), keyID, wrapped)
	if err != nil {
		return nil, fmt.Errorf("KMS unwrap failed for version %d: %w", version, err)
	}

	return plaintextDEK, nil
}

// CurrentVersion returns the active DEK version.
func (p *KMSEnvelopeKeyProvider) CurrentVersion() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentVersion
}

// ReWrapKeys re-wraps all stored DEKs under a new KMS master key ID without downtime (G-37).
func (p *KMSEnvelopeKeyProvider) ReWrapKeys(ctx context.Context, oldKeyID, newKeyID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for ver, wrapped := range p.wrappedDEKs {
		currentKeyID := p.keyIDs[ver]
		if currentKeyID == oldKeyID {
			newWrapped, err := p.kms.ReWrapKey(ctx, oldKeyID, newKeyID, wrapped)
			if err != nil {
				return fmt.Errorf("re-wrap DEK version %d: %w", ver, err)
			}
			p.wrappedDEKs[ver] = newWrapped
			p.keyIDs[ver] = newKeyID
		}
	}

	return nil
}

// AuditProcessEnvironment verifies that the master key material is not present in process env (G-36).
func AuditProcessEnvironment(forbiddenKeyPatterns []string) (bool, string) {
	envVars := os.Environ()
	for _, env := range envVars {
		parts := strings.SplitN(env, "=", 2)
		k := strings.ToUpper(parts[0])
		v := ""
		if len(parts) > 1 {
			v = parts[1]
		}

		for _, pat := range forbiddenKeyPatterns {
			upperPat := strings.ToUpper(pat)
			if strings.Contains(k, upperPat) && len(v) > 0 {
				return false, fmt.Sprintf("environment variable %s contains key material!", parts[0])
			}
		}
	}
	return true, ""
}
