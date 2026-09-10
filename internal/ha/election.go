package ha

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// State represents the high-availability state of a Control Plane instance.
type State string

const (
	StateStandby State = "STANDBY"
	StateLeader  State = "LEADER"
	StateFenced  State = "FENCED"
)

var (
	ErrNotLeader       = errors.New("instance is not the active leader; scheduling and mutation are fenced")
	ErrLockHeldByOther = errors.New("advisory lock is held by another instance")
	ErrLockLost        = errors.New("advisory lock was lost; stepping down immediately")
)

const (
	DefaultAdvisoryLockID int64 = 42424242
)

// AdvisoryLockProvider abstracts Postgres advisory lock mechanics.
type AdvisoryLockProvider interface {
	TryAcquire(ctx context.Context, lockID int64, ownerID string) (bool, error)
	RenewOrHold(ctx context.Context, lockID int64, ownerID string) (bool, error)
	Release(ctx context.Context, lockID int64, ownerID string) error
}

// MemoryAdvisoryLock provides an in-memory implementation of advisory locks with fault injection.
type MemoryAdvisoryLock struct {
	mu           sync.Mutex
	currentOwner string
	currentLock  int64
	lastRenew    time.Time
	leaseTTL     time.Duration
	partitions   map[string]bool // ownerID -> partitioned
}

func NewMemoryAdvisoryLock(leaseTTL time.Duration) *MemoryAdvisoryLock {
	if leaseTTL <= 0 {
		leaseTTL = 1 * time.Second
	}
	return &MemoryAdvisoryLock{
		leaseTTL:   leaseTTL,
		partitions: make(map[string]bool),
	}
}

func (m *MemoryAdvisoryLock) Partition(ownerID string, partitioned bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitions[ownerID] = partitioned
}

func (m *MemoryAdvisoryLock) TryAcquire(ctx context.Context, lockID int64, ownerID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.partitions[ownerID] {
		return false, fmt.Errorf("network partition between %s and lock store", ownerID)
	}

	now := time.Now()
	// Check if existing lease expired
	if m.currentOwner != "" && m.currentOwner != ownerID {
		if now.Sub(m.lastRenew) > m.leaseTTL {
			// Previous lease expired, allow takeover
			m.currentOwner = ""
		}
	}

	if m.currentOwner == "" || m.currentOwner == ownerID {
		m.currentOwner = ownerID
		m.currentLock = lockID
		m.lastRenew = now
		return true, nil
	}

	return false, nil
}

func (m *MemoryAdvisoryLock) RenewOrHold(ctx context.Context, lockID int64, ownerID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.partitions[ownerID] {
		return false, fmt.Errorf("network partition between %s and lock store", ownerID)
	}

	if m.currentOwner == ownerID && m.currentLock == lockID {
		m.lastRenew = time.Now()
		return true, nil
	}

	return false, nil
}

func (m *MemoryAdvisoryLock) Release(ctx context.Context, lockID int64, ownerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentOwner == ownerID && m.currentLock == lockID {
		m.currentOwner = ""
		m.currentLock = 0
	}
	return nil
}

func (m *MemoryAdvisoryLock) CurrentOwner() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentOwner
}

// PostgresAdvisoryLock implements real PostgreSQL advisory lock queries via pgxpool.
type PostgresAdvisoryLock struct {
	pool *pgxpool.Pool
}

func NewPostgresAdvisoryLock(pool *pgxpool.Pool) *PostgresAdvisoryLock {
	return &PostgresAdvisoryLock{pool: pool}
}

func (p *PostgresAdvisoryLock) TryAcquire(ctx context.Context, lockID int64, ownerID string) (bool, error) {
	if p.pool == nil {
		return false, fmt.Errorf("postgres pool not initialized")
	}
	var acquired bool
	err := p.pool.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
	return acquired, err
}

func (p *PostgresAdvisoryLock) RenewOrHold(ctx context.Context, lockID int64, ownerID string) (bool, error) {
	// In PostgreSQL session-level advisory locks remain held until explicitly unlocked or connection drops.
	// We run a ping/query to assert connection is alive.
	if p.pool == nil {
		return false, fmt.Errorf("postgres pool not initialized")
	}
	var res int
	err := p.pool.QueryRow(ctx, "SELECT 1").Scan(&res)
	return err == nil, err
}

func (p *PostgresAdvisoryLock) Release(ctx context.Context, lockID int64, ownerID string) error {
	if p.pool == nil {
		return nil
	}
	var unlocked bool
	_ = p.pool.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&unlocked)
	return nil
}

// DecisionRecord captures a scheduling decision timestamp for split-brain assertions.
type DecisionRecord struct {
	OwnerID   string
	Timestamp time.Time
}

// ElectorConfig sets parameters for HA election and failover.
type ElectorConfig struct {
	LockID       int64
	OwnerID      string
	PollInterval time.Duration
	RenewTimeout time.Duration
	LockProvider AdvisoryLockProvider
	Log          zerolog.Logger
}

// Elector manages leader election, failover promotion, and split-brain fencing (§16, Gates G-43, G-44).
type Elector struct {
	cfg        ElectorConfig
	state      atomic.Value // holds State
	onPromoted func(ctx context.Context)
	onDemoted  func()
	stopCh     chan struct{}
	stopped    atomic.Bool
	log        zerolog.Logger

	mu        sync.Mutex
	decisions []DecisionRecord
}

func NewElector(cfg ElectorConfig) *Elector {
	if cfg.LockID == 0 {
		cfg.LockID = DefaultAdvisoryLockID
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 25 * time.Millisecond
	}
	if cfg.RenewTimeout <= 0 {
		cfg.RenewTimeout = 100 * time.Millisecond
	}

	e := &Elector{
		cfg:       cfg,
		stopCh:    make(chan struct{}),
		log:       cfg.Log.With().Str("ha_owner", cfg.OwnerID).Logger(),
		decisions: make([]DecisionRecord, 0),
	}
	e.state.Store(StateStandby)
	return e
}

// OnPromoted registers a callback invoked upon acquiring active leadership.
func (e *Elector) OnPromoted(fn func(ctx context.Context)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onPromoted = fn
}

// OnDemoted registers a callback invoked upon losing leadership or stepping down.
func (e *Elector) OnDemoted(fn func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onDemoted = fn
}

// State returns current HA state (STANDBY, LEADER, FENCED).
func (e *Elector) State() State {
	return e.state.Load().(State)
}

// IsLeader returns true only if this instance currently holds active leadership.
func (e *Elector) IsLeader() bool {
	return e.State() == StateLeader
}

// Start launches the leader election campaign loop.
func (e *Elector) Start(ctx context.Context) {
	go e.campaignLoop(ctx)
}

// Stop shuts down the elector and releases advisory locks immediately.
func (e *Elector) Stop() {
	if e.stopped.CompareAndSwap(false, true) {
		close(e.stopCh)
		if e.IsLeader() {
			e.stepDown()
			_ = e.cfg.LockProvider.Release(context.Background(), e.cfg.LockID, e.cfg.OwnerID)
		}
	}
}

// StepDown deliberately relinquishes leadership (e.g. on partition from Postgres).
func (e *Elector) StepDown() {
	e.stepDown()
}

func (e *Elector) stepDown() {
	if e.state.CompareAndSwap(StateLeader, StateFenced) {
		e.log.Warn().Msg("stepping down from active leadership; fencing scheduling")
		e.mu.Lock()
		cb := e.onDemoted
		e.mu.Unlock()
		if cb != nil {
			cb()
		}
		e.state.Store(StateStandby)
	}
}

func (e *Elector) campaignLoop(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.Stop()
			return
		case <-e.stopCh:
			return
		case <-ticker.C:
			if e.IsLeader() {
				// Renew/Hold lock
				renewCtx, cancel := context.WithTimeout(ctx, e.cfg.RenewTimeout)
				ok, err := e.cfg.LockProvider.RenewOrHold(renewCtx, e.cfg.LockID, e.cfg.OwnerID)
				cancel()

				if err != nil || !ok {
					e.log.Warn().Err(err).Msg("advisory lock heartbeat failed; stepping down immediately")
					e.stepDown()
				}
			} else {
				// Attempt acquisition
				acqCtx, cancel := context.WithTimeout(ctx, e.cfg.RenewTimeout)
				acquired, err := e.cfg.LockProvider.TryAcquire(acqCtx, e.cfg.LockID, e.cfg.OwnerID)
				cancel()

				if err == nil && acquired {
					if e.state.CompareAndSwap(StateStandby, StateLeader) {
						e.log.Info().Msg("advisory lock acquired; PROMOTED to active Control Plane leader (G-43)")
						e.mu.Lock()
						cb := e.onPromoted
						e.mu.Unlock()
						if cb != nil {
							// Execute promotion sequence in separate goroutine or inline
							go cb(ctx)
						}
					}
				}
			}
		}
	}
}

// RecordDecision records a scheduling decision timestamp if active leader, otherwise rejects with ErrNotLeader.
func (e *Elector) RecordDecision() (time.Time, error) {
	if !e.IsLeader() {
		return time.Time{}, ErrNotLeader
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	e.decisions = append(e.decisions, DecisionRecord{
		OwnerID:   e.cfg.OwnerID,
		Timestamp: now,
	})
	return now, nil
}

// GetDecisions returns all recorded scheduling decision timestamps.
func (e *Elector) GetDecisions() []DecisionRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]DecisionRecord, len(e.decisions))
	copy(out, e.decisions)
	return out
}
