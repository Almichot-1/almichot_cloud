package deployments

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrDeploymentNotFound = errors.New("deployment not found")
	ErrInstanceNotFound   = errors.New("instance not found")
	ErrReleaseNotFound    = errors.New("release not found")
)

// DeploymentRepository defines storage operations for deployments.
type DeploymentRepository interface {
	Create(ctx context.Context, d *Deployment) error
	GetByID(ctx context.Context, id string) (*Deployment, error)
	UpdateStatus(ctx context.Context, id string, status DeploymentStatus, stage string) error
	UpdateImage(ctx context.Context, id string, image string, imageDigest string) error
	UpdateScale(ctx context.Context, id string, replicaCount int) error
	List(ctx context.Context, projectID string) ([]*Deployment, error)
}

// InstanceRepository defines storage operations for deployment instances.
type InstanceRepository interface {
	Create(ctx context.Context, inst *Instance) error
	GetByID(ctx context.Context, id string) (*Instance, error)
	GetByInstanceKey(ctx context.Context, key string) (*Instance, error)
	ListByDeployment(ctx context.Context, deploymentID string) ([]*Instance, error)
	ListByWorker(ctx context.Context, workerID string) ([]*Instance, error)
	Update(ctx context.Context, inst *Instance) error
	CountByWorkerForDeployment(ctx context.Context, deploymentID string) (map[string]int, error)
	ListAll(ctx context.Context) ([]*Instance, error)
}

// MemoryDeploymentRepository is an in-memory implementation of DeploymentRepository.
type MemoryDeploymentRepository struct {
	mu          sync.RWMutex
	deployments map[string]*Deployment
}

func NewMemoryDeploymentRepository() *MemoryDeploymentRepository {
	return &MemoryDeploymentRepository{
		deployments: make(map[string]*Deployment),
	}
}

func (r *MemoryDeploymentRepository) Create(ctx context.Context, d *Deployment) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = now
	}
	d.UpdatedAt = now

	clone := *d
	r.deployments[d.ID] = &clone
	return nil
}

func (r *MemoryDeploymentRepository) GetByID(ctx context.Context, id string) (*Deployment, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	d, ok := r.deployments[id]
	if !ok {
		return nil, ErrDeploymentNotFound
	}
	clone := *d
	return &clone, nil
}

func (r *MemoryDeploymentRepository) UpdateStatus(ctx context.Context, id string, status DeploymentStatus, stage string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	d, ok := r.deployments[id]
	if !ok {
		return ErrDeploymentNotFound
	}
	d.Status = status
	if stage != "" {
		d.Stage = stage
	}
	d.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *MemoryDeploymentRepository) UpdateImage(ctx context.Context, id string, image string, imageDigest string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	d, ok := r.deployments[id]
	if !ok {
		return ErrDeploymentNotFound
	}
	d.Image = image
	d.ImageDigest = imageDigest
	d.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *MemoryDeploymentRepository) UpdateScale(ctx context.Context, id string, replicaCount int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	d, ok := r.deployments[id]
	if !ok {
		return ErrDeploymentNotFound
	}
	d.InstanceCount = replicaCount
	d.DesiredReplicas = replicaCount
	d.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *MemoryDeploymentRepository) List(ctx context.Context, projectID string) ([]*Deployment, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]*Deployment, 0, len(r.deployments))
	for _, d := range r.deployments {
		if projectID == "" || d.ProjectID == projectID {
			clone := *d
			res = append(res, &clone)
		}
	}
	return res, nil
}

// Wipe clears all deployments from memory (used in disaster recovery and restore).
func (r *MemoryDeploymentRepository) Wipe(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deployments = make(map[string]*Deployment)
	return nil
}

// MemoryInstanceRepository is an in-memory implementation of InstanceRepository.
type MemoryInstanceRepository struct {
	mu        sync.RWMutex
	instances map[string]*Instance // by ID
	byKey     map[string]*Instance // by InstanceKey
}

func NewMemoryInstanceRepository() *MemoryInstanceRepository {
	return &MemoryInstanceRepository{
		instances: make(map[string]*Instance),
		byKey:     make(map[string]*Instance),
	}
}

func (r *MemoryInstanceRepository) Create(ctx context.Context, inst *Instance) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	if inst.CreatedAt.IsZero() {
		inst.CreatedAt = now
	}
	inst.UpdatedAt = now

	clone := *inst
	r.instances[inst.ID] = &clone
	r.byKey[inst.InstanceKey] = &clone
	return nil
}

func (r *MemoryInstanceRepository) GetByID(ctx context.Context, id string) (*Instance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	inst, ok := r.instances[id]
	if !ok {
		return nil, ErrInstanceNotFound
	}
	clone := *inst
	return &clone, nil
}

func (r *MemoryInstanceRepository) GetByInstanceKey(ctx context.Context, key string) (*Instance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	inst, ok := r.byKey[key]
	if !ok {
		return nil, ErrInstanceNotFound
	}
	clone := *inst
	return &clone, nil
}

func (r *MemoryInstanceRepository) ListByDeployment(ctx context.Context, deploymentID string) ([]*Instance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]*Instance, 0)
	for _, inst := range r.instances {
		if inst.DeploymentID == deploymentID {
			clone := *inst
			res = append(res, &clone)
		}
	}
	return res, nil
}

func (r *MemoryInstanceRepository) ListByWorker(ctx context.Context, workerID string) ([]*Instance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]*Instance, 0)
	for _, inst := range r.instances {
		if inst.WorkerID == workerID {
			clone := *inst
			res = append(res, &clone)
		}
	}
	return res, nil
}

func (r *MemoryInstanceRepository) Update(ctx context.Context, inst *Instance) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.instances[inst.ID]
	if !ok {
		return ErrInstanceNotFound
	}

	inst.UpdatedAt = time.Now().UTC()
	clone := *inst
	*existing = clone
	if inst.InstanceKey != "" {
		r.byKey[inst.InstanceKey] = existing
	}
	return nil
}

func (r *MemoryInstanceRepository) CountByWorkerForDeployment(ctx context.Context, deploymentID string) (map[string]int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	counts := make(map[string]int)
	for _, inst := range r.instances {
		if inst.DeploymentID == deploymentID && inst.WorkerID != "" {
			counts[inst.WorkerID]++
		}
	}
	return counts, nil
}

func (r *MemoryInstanceRepository) ListAll(ctx context.Context) ([]*Instance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]*Instance, 0, len(r.instances))
	for _, inst := range r.instances {
		clone := *inst
		res = append(res, &clone)
	}
	return res, nil
}

// Wipe clears all instances from memory (used in disaster recovery and restore).
func (r *MemoryInstanceRepository) Wipe(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.instances = make(map[string]*Instance)
	r.byKey = make(map[string]*Instance)
	return nil
}

// Release represents an immutable deployment release artifact (§10.1, §19.1, §21.3).
type Release struct {
	ID           string    `json:"id"`
	ProjectID    string    `json:"project_id"`
	DeploymentID string    `json:"deployment_id"`
	Version      string    `json:"version"`
	ImageRef     string    `json:"image_ref"`
	ImageDigest  string    `json:"image_digest"`
	Signature    string    `json:"signature"`
	CreatedAt    time.Time `json:"created_at"`
}

// ReleaseRepository provides access to durable release records.
type ReleaseRepository interface {
	Create(ctx context.Context, release *Release) error
	Get(ctx context.Context, id string) (*Release, error)
	GetByDigest(ctx context.Context, digest string) (*Release, error)
	ListByProject(ctx context.Context, projectID string) ([]*Release, error)
}

// MemoryReleaseRepository is an in-memory implementation of ReleaseRepository.
type MemoryReleaseRepository struct {
	mu       sync.RWMutex
	releases map[string]*Release
}

func NewMemoryReleaseRepository() *MemoryReleaseRepository {
	return &MemoryReleaseRepository{
		releases: make(map[string]*Release),
	}
}

func (r *MemoryReleaseRepository) Create(ctx context.Context, rel *Release) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if rel.ID == "" {
		rel.ID = uuid.New().String()
	}
	if rel.CreatedAt.IsZero() {
		rel.CreatedAt = time.Now().UTC()
	}

	clone := *rel
	r.releases[rel.ID] = &clone
	return nil
}

func (r *MemoryReleaseRepository) Get(ctx context.Context, id string) (*Release, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rel, ok := r.releases[id]
	if !ok {
		return nil, ErrReleaseNotFound
	}
	clone := *rel
	return &clone, nil
}

func (r *MemoryReleaseRepository) GetByDigest(ctx context.Context, digest string) (*Release, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, rel := range r.releases {
		if rel.ImageDigest == digest {
			clone := *rel
			return &clone, nil
		}
	}
	return nil, ErrReleaseNotFound
}

func (r *MemoryReleaseRepository) ListByProject(ctx context.Context, projectID string) ([]*Release, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*Release
	for _, rel := range r.releases {
		if rel.ProjectID == projectID {
			clone := *rel
			result = append(result, &clone)
		}
	}
	return result, nil
}

// List returns all releases across all projects if projectID is empty.
func (r *MemoryReleaseRepository) List(ctx context.Context, projectID string) ([]*Release, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*Release
	for _, rel := range r.releases {
		if projectID == "" || rel.ProjectID == projectID {
			clone := *rel
			result = append(result, &clone)
		}
	}
	return result, nil
}

// Wipe clears all releases from memory (used in disaster recovery and restore).
func (r *MemoryReleaseRepository) Wipe(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releases = make(map[string]*Release)
	return nil
}

