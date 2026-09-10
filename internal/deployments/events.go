package deployments

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Event represents an operational lifecycle event (§19.1, §22, §27.2).
type Event struct {
	ID           string                 `json:"id"`
	ProjectID    string                 `json:"project_id"`
	DeploymentID string                 `json:"deployment_id"`
	EventType    string                 `json:"event_type"`
	Message      string                 `json:"message"`
	Metadata     map[string]interface{} `json:"metadata"`
	CreatedAt    time.Time              `json:"created_at"`
}

// EventRepository provides access to operational event records.
type EventRepository interface {
	Create(ctx context.Context, event *Event) error
	ListByProject(ctx context.Context, projectID string) ([]*Event, error)
	ListByDeployment(ctx context.Context, deploymentID string) ([]*Event, error)
}

// MemoryEventRepository is an in-memory implementation of EventRepository for testing.
type MemoryEventRepository struct {
	mu     sync.RWMutex
	events []*Event
}

// NewMemoryEventRepository creates a new in-memory EventRepository.
func NewMemoryEventRepository() *MemoryEventRepository {
	return &MemoryEventRepository{
		events: make([]*Event, 0),
	}
}

func (r *MemoryEventRepository) Create(ctx context.Context, ev *Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ev.ID == "" {
		ev.ID = uuid.New().String()
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}
	if ev.Metadata == nil {
		ev.Metadata = make(map[string]interface{})
	}

	clone := *ev
	r.events = append(r.events, &clone)
	return nil
}

func (r *MemoryEventRepository) ListByProject(ctx context.Context, projectID string) ([]*Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []*Event
	for i := len(r.events) - 1; i >= 0; i-- {
		ev := r.events[i]
		if ev.ProjectID == projectID {
			clone := *ev
			res = append(res, &clone)
		}
	}
	return res, nil
}

func (r *MemoryEventRepository) ListByDeployment(ctx context.Context, deploymentID string) ([]*Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []*Event
	for i := len(r.events) - 1; i >= 0; i-- {
		ev := r.events[i]
		if ev.DeploymentID == deploymentID {
			clone := *ev
			res = append(res, &clone)
		}
	}
	return res, nil
}

// ListAll returns all recorded events.
func (r *MemoryEventRepository) ListAll(ctx context.Context) ([]*Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []*Event
	for _, ev := range r.events {
		clone := *ev
		res = append(res, &clone)
	}
	return res, nil
}

// Wipe clears all events from memory (used in disaster recovery and restore).
func (r *MemoryEventRepository) Wipe(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = make([]*Event, 0)
	return nil
}
