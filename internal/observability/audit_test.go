package observability

import (
	"context"
	"errors"
	"testing"
)

func TestAudit_AppendAndVerifyChain(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryAuditRepository()

	// Append events of different categories
	e0, err := repo.Append(ctx, EventAuthLogin, "alice", "auth-system", map[string]string{"ip": "192.168.1.10"})
	if err != nil {
		t.Fatalf("Append e0 failed: %v", err)
	}
	if e0.PrevHash != GenesisHash {
		t.Errorf("Genesis entry prev_hash expected %s, got %s", GenesisHash, e0.PrevHash)
	}

	e1, err := repo.Append(ctx, EventSecretCreate, "alice", "proj-alpha/DATABASE_URL", map[string]string{"version": "1"})
	if err != nil {
		t.Fatalf("Append e1 failed: %v", err)
	}
	if e1.PrevHash != e0.Hash {
		t.Errorf("e1 prev_hash expected %s, got %s", e0.Hash, e1.PrevHash)
	}

	e2, err := repo.Append(ctx, EventSecretRotate, "bob", "proj-alpha/DATABASE_URL", map[string]string{"version": "2"})
	if err != nil {
		t.Fatalf("Append e2 failed: %v", err)
	}
	if e2.PrevHash != e1.Hash {
		t.Errorf("e2 prev_hash expected %s, got %s", e1.Hash, e2.PrevHash)
	}

	e3, err := repo.Append(ctx, EventRBACRoleAssign, "admin", "bob", map[string]string{"role": "operator"})
	if err != nil {
		t.Fatalf("Append e3 failed: %v", err)
	}
	if e3.PrevHash != e2.Hash {
		t.Errorf("e3 prev_hash expected %s, got %s", e2.Hash, e3.PrevHash)
	}

	// Chain verification should succeed
	if err := repo.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain failed on untampered log: %v", err)
	}
}

func TestAudit_ImmutableEnforcement(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryAuditRepository()

	entry, _ := repo.Append(ctx, EventAuthLogin, "alice", "system", nil)

	// Attempt Update -> should be rejected with ErrAuditImmutable
	err := repo.Update(ctx, entry)
	if !errors.Is(err, ErrAuditImmutable) {
		t.Errorf("Expected ErrAuditImmutable on Update, got: %v", err)
	}

	// Attempt Delete -> should be rejected with ErrAuditImmutable
	err = repo.Delete(ctx, entry.Index)
	if !errors.Is(err, ErrAuditImmutable) {
		t.Errorf("Expected ErrAuditImmutable on Delete, got: %v", err)
	}
}

func TestAudit_TamperDetection(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryAuditRepository()

	for i := 0; i < 5; i++ {
		_, _ = repo.Append(ctx, EventSecretAccess, "service-account", "secret-key", map[string]string{"idx": string(rune('0' + i))})
	}

	// Verify before tamper
	if err := repo.VerifyChain(ctx); err != nil {
		t.Fatalf("Pre-tamper verification failed: %v", err)
	}

	// Tamper entry index 2
	if err := repo.TamperEntryForTest(2, "malicious-actor"); err != nil {
		t.Fatalf("TamperEntryForTest failed: %v", err)
	}

	// Verify after tamper -> must fail with ErrAuditTampered
	err := repo.VerifyChain(ctx)
	if err == nil {
		t.Fatal("Expected VerifyChain to fail on tampered record, but it succeeded!")
	}
	if !errors.Is(err, ErrAuditTampered) {
		t.Errorf("Expected ErrAuditTampered, got: %v", err)
	}
}

func TestAudit_DeletionDetection(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryAuditRepository()

	for i := 0; i < 5; i++ {
		_, _ = repo.Append(ctx, EventAuthTokenIssue, "user", "token-svc", nil)
	}

	// Delete entry index 2 directly from storage
	if err := repo.DeleteEntryForTest(2); err != nil {
		t.Fatalf("DeleteEntryForTest failed: %v", err)
	}

	// Verify after deletion -> must fail with broken chain
	err := repo.VerifyChain(ctx)
	if err == nil {
		t.Fatal("Expected VerifyChain to fail on deleted record, but it succeeded!")
	}
	if !errors.Is(err, ErrAuditBrokenChain) {
		t.Errorf("Expected ErrAuditBrokenChain, got: %v", err)
	}
}
