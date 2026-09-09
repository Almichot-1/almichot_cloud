// Package projects defines the project entity and its storage contract.
package projects

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrProjectNotFound is returned when no project matches the requested key.
var ErrProjectNotFound = errors.New("project not found")

// Project represents an application under deployment.
type Project struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Description   string    `json:"description"`
	RepoURL       string    `json:"repo_url"`
	WebhookSecret string    `json:"webhook_secret,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// ProjectRepository defines storage operations for projects.
type ProjectRepository interface {
	Create(ctx context.Context, p *Project) error
	GetByID(ctx context.Context, id string) (*Project, error)
	GetByName(ctx context.Context, name string) (*Project, error)
	List(ctx context.Context) ([]*Project, error)
}

// MemoryProjectRepository is an in-memory implementation of ProjectRepository.
type MemoryProjectRepository struct {
	mu       sync.RWMutex
	projects map[string]*Project
	byName   map[string]*Project
}

// NewMemoryProjectRepository creates a new in-memory project repository.
func NewMemoryProjectRepository() *MemoryProjectRepository {
	return &MemoryProjectRepository{
		projects: make(map[string]*Project),
		byName:   make(map[string]*Project),
	}
}

func (r *MemoryProjectRepository) Create(ctx context.Context, p *Project) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if p.Name == "" {
		return errors.New("project name is required")
	}
	if _, exists := r.byName[p.Name]; exists {
		return errors.New("project name already exists")
	}

	now := time.Now().UTC()
	p.CreatedAt = now
	p.UpdatedAt = now

	clone := *p
	r.projects[p.ID] = &clone
	r.byName[p.Name] = &clone
	return nil
}

func (r *MemoryProjectRepository) GetByID(ctx context.Context, id string) (*Project, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, ok := r.projects[id]
	if !ok {
		return nil, ErrProjectNotFound
	}
	clone := *p
	return &clone, nil
}

func (r *MemoryProjectRepository) GetByName(ctx context.Context, name string) (*Project, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, ok := r.byName[name]
	if !ok {
		return nil, ErrProjectNotFound
	}
	clone := *p
	return &clone, nil
}

func (r *MemoryProjectRepository) List(ctx context.Context) ([]*Project, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]*Project, 0, len(r.projects))
	for _, p := range r.projects {
		clone := *p
		res = append(res, &clone)
	}
	return res, nil
}