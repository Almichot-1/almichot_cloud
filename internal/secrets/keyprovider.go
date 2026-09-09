package secrets

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrKeyNotFound is returned when a requested key version does not exist.
	ErrKeyNotFound = errors.New("encryption key not found for version")
	// ErrInvalidKeyLength is returned when a key is not 32 bytes (256 bits).
	ErrInvalidKeyLength = errors.New("encryption key must be 32 bytes for AES-256")
)

// KeyProvider manages encryption keys across multiple key versions.
type KeyProvider interface {
	GetKey(version int) ([]byte, error)
	CurrentVersion() int
}

// StaticKeyProvider implements KeyProvider backed by a map of versioned 32-byte keys.
type StaticKeyProvider struct {
	mu             sync.RWMutex
	currentVersion int
	keys           map[int][]byte
}

// NewStaticKeyProvider creates a key provider with a map of key versions.
func NewStaticKeyProvider(keys map[int][]byte, currentVersion int) (*StaticKeyProvider, error) {
	if len(keys) == 0 {
		return nil, errors.New("at least one key must be provided")
	}
	validated := make(map[int][]byte)
	for ver, key := range keys {
		if len(key) != 32 {
			return nil, fmt.Errorf("version %d: %w (got %d bytes)", ver, ErrInvalidKeyLength, len(key))
		}
		k := make([]byte, 32)
		copy(k, key)
		validated[ver] = k
	}

	if _, ok := validated[currentVersion]; !ok {
		return nil, fmt.Errorf("current version %d not found in key map", currentVersion)
	}

	return &StaticKeyProvider{
		currentVersion: currentVersion,
		keys:           validated,
	}, nil
}

// NewSingleKeyProvider creates a key provider with a single key at version 1.
// If the key is not 32 bytes, it derives a 32-byte key using SHA-256.
func NewSingleKeyProvider(rawKey []byte) *StaticKeyProvider {
	var key32 []byte
	if len(rawKey) == 32 {
		key32 = make([]byte, 32)
		copy(key32, rawKey)
	} else {
		hash := sha256.Sum256(rawKey)
		key32 = hash[:]
	}

	return &StaticKeyProvider{
		currentVersion: 1,
		keys: map[int][]byte{
			1: key32,
		},
	}
}

// GetKey returns the 32-byte key for the specified version.
func (p *StaticKeyProvider) GetKey(version int) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	k, ok := p.keys[version]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrKeyNotFound, version)
	}
	res := make([]byte, len(k))
	copy(res, k)
	return res, nil
}

// CurrentVersion returns the active key version for new encryptions.
func (p *StaticKeyProvider) CurrentVersion() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentVersion
}

// AddKey registers a new key version and optionally marks it as current.
func (p *StaticKeyProvider) AddKey(version int, key []byte, makeCurrent bool) error {
	if len(key) != 32 {
		return ErrInvalidKeyLength
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	k := make([]byte, 32)
	copy(k, key)
	p.keys[version] = k
	if makeCurrent {
		p.currentVersion = version
	}
	return nil
}
