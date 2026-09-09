package api

import (
	"net/http"
	"sync"
)

// IdempotencyStatus represents the state of a request associated with an idempotency key.
type IdempotencyStatus string

const (
	IdempotencyInFlight  IdempotencyStatus = "IN_FLIGHT"
	IdempotencyCompleted IdempotencyStatus = "COMPLETED"
)

// IdempotencyEntry stores the result or in-flight promise of a request.
type IdempotencyEntry struct {
	mu     sync.RWMutex
	Status IdempotencyStatus
	Code   int
	Header http.Header
	Body   []byte
	Done   chan struct{}
}

// IdempotencyStore manages request deduplication and cached responses (RACE-02, Gate G-22).
type IdempotencyStore struct {
	mu      sync.RWMutex
	entries map[string]*IdempotencyEntry
}

// NewIdempotencyStore creates a new in-memory idempotency store.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{
		entries: make(map[string]*IdempotencyEntry),
	}
}

// GetOrReserve atomically checks if a key is already in-flight or completed.
// Returns (entry, true) if this is the first caller responsible for executing the request.
// Returns (entry, false) if this is a concurrent or repeated duplicate request.
func (s *IdempotencyStore) GetOrReserve(key string) (*IdempotencyEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, exists := s.entries[key]; exists {
		return entry, false
	}

	entry := &IdempotencyEntry{
		Status: IdempotencyInFlight,
		Done:   make(chan struct{}),
	}
	s.entries[key] = entry
	return entry, true
}

// Complete finalizes an in-flight entry with the response status, headers, and body,
// and unblocks any concurrent callers waiting on the result.
func (s *IdempotencyStore) Complete(key string, code int, header http.Header, body []byte) {
	s.mu.RLock()
	entry, exists := s.entries[key]
	s.mu.RUnlock()

	if !exists {
		return
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.Status = IdempotencyCompleted
	entry.Code = code
	entry.Header = header.Clone()
	entry.Body = append([]byte(nil), body...)

	close(entry.Done)
}

// WaitAndGet waits for an in-flight entry to complete and returns its response.
func (e *IdempotencyEntry) WaitAndGet() (int, http.Header, []byte) {
	<-e.Done

	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.Code, e.Header.Clone(), append([]byte(nil), e.Body...)
}
