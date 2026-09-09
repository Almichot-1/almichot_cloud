package workers

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrWorkerNotFound = errors.New("worker not found")
)

// WorkerRepository defines data access methods for Worker records.
type WorkerRepository interface {
	Upsert(ctx context.Context, w *Worker) error
	GetByID(ctx context.Context, id string) (*Worker, error)
	GetByWorkerKey(ctx context.Context, key string) (*Worker, error)
	List(ctx context.Context) ([]*Worker, error)
	UpdateHeartbeat(ctx context.Context, workerKey string, beatAt time.Time) error
	SetDrain(ctx context.Context, workerKey string, draining bool) error
	UpdateActiveWorkloads(ctx context.Context, workerKey string, delta int) error
}

// MemoryWorkerRepository is an in-memory thread-safe implementation of WorkerRepository.
type MemoryWorkerRepository struct {
	mu      sync.RWMutex
	workers map[string]*Worker // keyed by worker_key
	byID    map[string]*Worker // keyed by id
}

// NewMemoryWorkerRepository creates a new in-memory worker repository.
func NewMemoryWorkerRepository() *MemoryWorkerRepository {
	return &MemoryWorkerRepository{
		workers: make(map[string]*Worker),
		byID:    make(map[string]*Worker),
	}
}

func (r *MemoryWorkerRepository) Upsert(ctx context.Context, w *Worker) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	if w.CreatedAt.IsZero() {
		w.CreatedAt = now
	}
	w.UpdatedAt = now

	clone := *w
	// Deep copy labels
	if w.Labels != nil {
		clone.Labels = make(map[string]string, len(w.Labels))
		for k, v := range w.Labels {
			clone.Labels[k] = v
		}
	}

	r.workers[w.WorkerKey] = &clone
	if w.ID != "" {
		r.byID[w.ID] = &clone
	}
	return nil
}

func (r *MemoryWorkerRepository) GetByID(ctx context.Context, id string) (*Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	w, ok := r.byID[id]
	if !ok {
		return nil, ErrWorkerNotFound
	}
	clone := *w
	return &clone, nil
}

func (r *MemoryWorkerRepository) GetByWorkerKey(ctx context.Context, key string) (*Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	w, ok := r.workers[key]
	if !ok {
		return nil, ErrWorkerNotFound
	}
	clone := *w
	return &clone, nil
}

func (r *MemoryWorkerRepository) List(ctx context.Context) ([]*Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]*Worker, 0, len(r.workers))
	for _, w := range r.workers {
		clone := *w
		res = append(res, &clone)
	}
	return res, nil
}

func (r *MemoryWorkerRepository) UpdateHeartbeat(ctx context.Context, workerKey string, beatAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.workers[workerKey]
	if !ok {
		return ErrWorkerNotFound
	}
	w.LastBeatAt = &beatAt
	w.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *MemoryWorkerRepository) SetDrain(ctx context.Context, workerKey string, draining bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.workers[workerKey]
	if !ok {
		return ErrWorkerNotFound
	}
	if draining {
		w.State = StateDraining
		w.Schedulable = false
	} else {
		w.State = StateReady
		w.Schedulable = true
	}
	w.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *MemoryWorkerRepository) UpdateActiveWorkloads(ctx context.Context, workerKey string, delta int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.workers[workerKey]
	if !ok {
		return ErrWorkerNotFound
	}
	w.ActiveWorkloads += delta
	if w.ActiveWorkloads < 0 {
		w.ActiveWorkloads = 0
	}
	w.UpdatedAt = time.Now().UTC()
	return nil
}

var _ WorkerRepository = (*MemoryWorkerRepository)(nil)
