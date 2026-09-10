package ha

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestHA_AdvisoryLockProvider_AcquireAndRelease(t *testing.T) {
	ctx := context.Background()
	provider := NewMemoryAdvisoryLock(500 * time.Millisecond)

	// Instance 1 acquires
	ok, err := provider.TryAcquire(ctx, 100, "cp-1")
	if err != nil || !ok {
		t.Fatalf("cp-1 failed to acquire lock: %v", err)
	}

	// Instance 2 cannot acquire while held
	ok, err = provider.TryAcquire(ctx, 100, "cp-2")
	if err != nil || ok {
		t.Fatalf("cp-2 unexpectedly acquired held lock: ok=%v, err=%v", ok, err)
	}

	// Instance 1 renews
	ok, err = provider.RenewOrHold(ctx, 100, "cp-1")
	if err != nil || !ok {
		t.Fatalf("cp-1 failed to renew lock: %v", err)
	}

	// Instance 1 releases
	if err := provider.Release(ctx, 100, "cp-1"); err != nil {
		t.Fatalf("cp-1 failed to release lock: %v", err)
	}

	// Instance 2 can now acquire
	ok, err = provider.TryAcquire(ctx, 100, "cp-2")
	if err != nil || !ok {
		t.Fatalf("cp-2 failed to acquire released lock: %v", err)
	}
}

func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestHA_StandbyPromotesOnLeaderDeath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := zerolog.Nop()
	lockProvider := NewMemoryAdvisoryLock(1 * time.Second)

	// Setup CP-1
	cp1 := NewElector(ElectorConfig{
		LockID:       101,
		OwnerID:      "cp-1",
		PollInterval: 15 * time.Millisecond,
		RenewTimeout: 50 * time.Millisecond,
		LockProvider: lockProvider,
		Log:          log,
	})

	// Setup CP-2 (Standby)
	cp2 := NewElector(ElectorConfig{
		LockID:       101,
		OwnerID:      "cp-2",
		PollInterval: 15 * time.Millisecond,
		RenewTimeout: 50 * time.Millisecond,
		LockProvider: lockProvider,
		Log:          log,
	})

	cp1.Start(ctx)

	// Wait for CP-1 to win leadership
	if !waitFor(300*time.Millisecond, cp1.IsLeader) {
		t.Fatal("Expected cp-1 to be active leader")
	}

	cp2.Start(ctx)
	time.Sleep(20 * time.Millisecond)

	if cp2.IsLeader() {
		t.Fatal("Expected cp-2 to be standby while cp-1 is leader")
	}

	// Kill CP-1
	cp1.Stop()

	// Wait for CP-2 to detect lock release and promote
	if !waitFor(500*time.Millisecond, cp2.IsLeader) {
		t.Fatal("Expected cp-2 to be promoted to active leader after cp-1 stopped")
	}

	cp2.Stop()
}

func TestHA_PartitionFromPostgresTriggersStepDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := zerolog.Nop()
	lockProvider := NewMemoryAdvisoryLock(100 * time.Millisecond)

	cp1 := NewElector(ElectorConfig{
		LockID:       102,
		OwnerID:      "cp-1",
		PollInterval: 15 * time.Millisecond,
		RenewTimeout: 30 * time.Millisecond,
		LockProvider: lockProvider,
		Log:          log,
	})

	cp2 := NewElector(ElectorConfig{
		LockID:       102,
		OwnerID:      "cp-2",
		PollInterval: 15 * time.Millisecond,
		RenewTimeout: 30 * time.Millisecond,
		LockProvider: lockProvider,
		Log:          log,
	})

	cp1.Start(ctx)

	if !waitFor(300*time.Millisecond, cp1.IsLeader) {
		t.Fatal("cp-1 must be leader")
	}

	cp2.Start(ctx)

	// Partition CP-1 from lock provider (simulates network partition from Postgres)
	lockProvider.Partition("cp-1", true)

	// CP-1 renew will fail, causing it to step down
	if !waitFor(300*time.Millisecond, func() bool { return !cp1.IsLeader() }) {
		t.Fatal("cp-1 should have stepped down after being partitioned from Postgres")
	}

	// CP-2 should acquire and become leader
	if !waitFor(500*time.Millisecond, cp2.IsLeader) {
		t.Fatal("cp-2 should have promoted to leader after cp-1 partition")
	}

	cp1.Stop()
	cp2.Stop()
}

func TestHA_FencingRejectsNonLeaderDecisions(t *testing.T) {
	log := zerolog.Nop()
	lockProvider := NewMemoryAdvisoryLock(1 * time.Second)

	cp := NewElector(ElectorConfig{
		LockID:       103,
		OwnerID:      "cp-standby",
		LockProvider: lockProvider,
		Log:          log,
	})

	// Do not start or acquire lock -> remains in standby
	_, err := cp.RecordDecision()
	if err == nil || err != ErrNotLeader {
		t.Fatalf("Expected ErrNotLeader for standby instance, got: %v", err)
	}
}
