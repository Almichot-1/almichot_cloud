package runtime

import (
	"sync"
	"time"
)

// InstanceState represents the lifecycle state of a tracked instance.
type InstanceState string

const (
	StatePending  InstanceState = "PENDING"
	StateStarting InstanceState = "STARTING"
	StateRunning  InstanceState = "RUNNING"
	StateStopping InstanceState = "STOPPING"
	StateStopped  InstanceState = "STOPPED"
	StateFailed   InstanceState = "FAILED"
)

// InstanceRecord represents the state of an instance on the local worker.
type InstanceRecord struct {
	InstanceID   string
	DeploymentID string
	ContainerID  string
	Image        string
	State        InstanceState
	ExitCode     int
	LastError    string
	Labels       map[string]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// InstanceTracker provides concurrency-safe local state management and keyed synchronization
// to guarantee G-09 (one instance identity -> max one container).
type InstanceTracker struct {
	globalMu  sync.RWMutex
	records   map[string]*InstanceRecord
	keyLocks  map[string]*keyedLock
	keyLockMu sync.Mutex
}

type keyedLock struct {
	mu       sync.Mutex
	refCount int
}

// NewInstanceTracker initializes a new tracker.
func NewInstanceTracker() *InstanceTracker {
	return &InstanceTracker{
		records:  make(map[string]*InstanceRecord),
		keyLocks: make(map[string]*keyedLock),
	}
}

// LockInstance locks synchronization for a specific instance ID.
// It returns an unlock function that must be deferred.
func (t *InstanceTracker) LockInstance(instanceID string) func() {
	t.keyLockMu.Lock()
	kl, exists := t.keyLocks[instanceID]
	if !exists {
		kl = &keyedLock{}
		t.keyLocks[instanceID] = kl
	}
	kl.refCount++
	t.keyLockMu.Unlock()

	kl.mu.Lock()

	return func() {
		kl.mu.Unlock()

		t.keyLockMu.Lock()
		kl.refCount--
		if kl.refCount == 0 {
			delete(t.keyLocks, instanceID)
		}
		t.keyLockMu.Unlock()
	}
}

// Get returns a copy of the instance record for the given instance ID.
func (t *InstanceTracker) Get(instanceID string) (*InstanceRecord, bool) {
	t.globalMu.RLock()
	defer t.globalMu.RUnlock()

	rec, ok := t.records[instanceID]
	if !ok {
		return nil, false
	}
	cpy := *rec
	return &cpy, true
}

// Set saves or updates an instance record.
func (t *InstanceTracker) Set(rec *InstanceRecord) {
	t.globalMu.Lock()
	defer t.globalMu.Unlock()

	now := time.Now().UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	rec.UpdatedAt = now

	cpy := *rec
	t.records[rec.InstanceID] = &cpy
}

// Delete removes an instance record from tracking.
func (t *InstanceTracker) Delete(instanceID string) {
	t.globalMu.Lock()
	defer t.globalMu.Unlock()

	delete(t.records, instanceID)
}

// List returns copies of all currently tracked instance records.
func (t *InstanceTracker) List() []*InstanceRecord {
	t.globalMu.RLock()
	defer t.globalMu.RUnlock()

	res := make([]*InstanceRecord, 0, len(t.records))
	for _, rec := range t.records {
		cpy := *rec
		res = append(res, &cpy)
	}
	return res
}
