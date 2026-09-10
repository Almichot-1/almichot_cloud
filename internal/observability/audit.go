package observability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrAuditImmutable   = errors.New("audit log is append-only; update and delete operations are strictly prohibited")
	ErrAuditTampered    = errors.New("audit chain verification failed: record has been tampered with or modified")
	ErrAuditBrokenChain = errors.New("audit chain verification failed: prev_hash does not match previous entry's hash")
	ErrAuditNotFound    = errors.New("audit record not found")
)

const (
	GenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

	// Event Categories
	EventAuthLogin       = "AUTH_LOGIN"
	EventAuthTokenIssue  = "AUTH_TOKEN_ISSUE"
	EventAuthTokenExpire = "AUTH_TOKEN_EXPIRE"
	EventAuthLoginFailed = "AUTH_LOGIN_FAILED"

	EventSecretCreate = "SECRET_CREATE"
	EventSecretAccess = "SECRET_ACCESS"
	EventSecretRotate = "SECRET_ROTATE"
	EventSecretDelete = "SECRET_DELETE"

	EventRBACRoleAssign    = "RBAC_ROLE_ASSIGN"
	EventRBACRoleRevoke    = "RBAC_ROLE_REVOKE"
	EventRBACPolicyChanged = "RBAC_POLICY_CHANGE"
)

// AuditEntry represents a single tamper-evident, hash-chained audit record (§22).
type AuditEntry struct {
	Index     uint64            `json:"index"`
	Timestamp time.Time         `json:"timestamp"`
	EventType string            `json:"event_type"`
	Actor     string            `json:"actor"`
	Resource  string            `json:"resource"`
	Details   map[string]string `json:"details,omitempty"`
	PrevHash  string            `json:"prev_hash"`
	Hash      string            `json:"hash"`
}

// ComputeHash calculates SHA-256 hash across canonical fields of the audit entry.
func ComputeHash(index uint64, ts time.Time, eventType, actor, resource string, details map[string]string, prevHash string) string {
	keys := make([]string, 0, len(details))
	for k := range details {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var dParts []string
	for _, k := range keys {
		dParts = append(dParts, fmt.Sprintf("%s=%s", k, details[k]))
	}
	detailsCanonical := strings.Join(dParts, "&")

	raw := fmt.Sprintf("%d|%s|%s|%s|%s|%s|%s",
		index,
		ts.UTC().Format(time.RFC3339Nano),
		eventType,
		actor,
		resource,
		detailsCanonical,
		prevHash,
	)

	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

// AuditRepository defines the append-only, tamper-evident audit store interface.
type AuditRepository interface {
	Append(ctx context.Context, eventType, actor, resource string, details map[string]string) (*AuditEntry, error)
	Get(ctx context.Context, index uint64) (*AuditEntry, error)
	List(ctx context.Context, limit, offset int) ([]*AuditEntry, error)
	VerifyChain(ctx context.Context) error
	Update(ctx context.Context, entry *AuditEntry) error
	Delete(ctx context.Context, index uint64) error
	Count(ctx context.Context) (int, error)
}

// MemoryAuditRepository implements an in-memory, append-only, tamper-evident audit log.
type MemoryAuditRepository struct {
	mu      sync.RWMutex
	entries []*AuditEntry
}

func NewMemoryAuditRepository() *MemoryAuditRepository {
	return &MemoryAuditRepository{
		entries: make([]*AuditEntry, 0),
	}
}

// Append adds a new audit entry to the hash chain.
func (r *MemoryAuditRepository) Append(ctx context.Context, eventType, actor, resource string, details map[string]string) (*AuditEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx := uint64(len(r.entries))
	prevHash := GenesisHash
	if idx > 0 {
		prevHash = r.entries[idx-1].Hash
	}

	ts := time.Now().UTC()

	// Clone details
	detailsCopy := make(map[string]string)
	for k, v := range details {
		detailsCopy[k] = v
	}

	hash := ComputeHash(idx, ts, eventType, actor, resource, detailsCopy, prevHash)

	entry := &AuditEntry{
		Index:     idx,
		Timestamp: ts,
		EventType: eventType,
		Actor:     actor,
		Resource:  resource,
		Details:   detailsCopy,
		PrevHash:  prevHash,
		Hash:      hash,
	}

	r.entries = append(r.entries, entry)
	return entry, nil
}

// Get retrieves an audit record by index.
func (r *MemoryAuditRepository) Get(ctx context.Context, index uint64) (*AuditEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if index >= uint64(len(r.entries)) {
		return nil, ErrAuditNotFound
	}
	e := *r.entries[index]
	return &e, nil
}

// List returns a slice of audit records.
func (r *MemoryAuditRepository) List(ctx context.Context, limit, offset int) ([]*AuditEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if offset >= len(r.entries) {
		return []*AuditEntry{}, nil
	}
	end := offset + limit
	if limit <= 0 || end > len(r.entries) {
		end = len(r.entries)
	}

	res := make([]*AuditEntry, end-offset)
	for i := offset; i < end; i++ {
		e := *r.entries[i]
		res[i-offset] = &e
	}
	return res, nil
}

// Count returns the total number of audit records.
func (r *MemoryAuditRepository) Count(ctx context.Context) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries), nil
}

// VerifyChain iterates from index 0 to the tip, verifying that every entry's hash
// matches its contents and links to the previous entry's hash.
func (r *MemoryAuditRepository) VerifyChain(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	expectedPrev := GenesisHash
	for i, entry := range r.entries {
		if entry.Index != uint64(i) {
			return fmt.Errorf("%w: index mismatch at entry %d (found %d)", ErrAuditBrokenChain, i, entry.Index)
		}

		if entry.PrevHash != expectedPrev {
			return fmt.Errorf("%w: entry %d prev_hash %s != expected %s", ErrAuditBrokenChain, i, entry.PrevHash, expectedPrev)
		}

		recalc := ComputeHash(entry.Index, entry.Timestamp, entry.EventType, entry.Actor, entry.Resource, entry.Details, entry.PrevHash)
		if entry.Hash != recalc {
			return fmt.Errorf("%w: entry %d hash %s != calculated %s", ErrAuditTampered, i, entry.Hash, recalc)
		}

		expectedPrev = entry.Hash
	}

	return nil
}

// Update rejects modification to enforce append-only invariant.
func (r *MemoryAuditRepository) Update(ctx context.Context, entry *AuditEntry) error {
	return ErrAuditImmutable
}

// Delete rejects deletion to enforce append-only invariant.
func (r *MemoryAuditRepository) Delete(ctx context.Context, index uint64) error {
	return ErrAuditImmutable
}

// TamperEntryForTest simulates storage-layer tampering for tests (e.g. unauthorized direct DB update).
func (r *MemoryAuditRepository) TamperEntryForTest(index uint64, tamperedActor string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index >= uint64(len(r.entries)) {
		return ErrAuditNotFound
	}
	r.entries[index].Actor = tamperedActor
	return nil
}

// DeleteEntryForTest simulates storage-layer deletion for tests (e.g. unauthorized direct DB delete).
func (r *MemoryAuditRepository) DeleteEntryForTest(index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index >= uint64(len(r.entries)) {
		return ErrAuditNotFound
	}
	r.entries = append(r.entries[:index], r.entries[index+1:]...)
	return nil
}
