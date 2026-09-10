package deployments

import "time"

type DeploymentStatus string

const (
	StatusQueued     DeploymentStatus = "QUEUED"
	StatusBuilding   DeploymentStatus = "BUILDING"
	StatusBuilt      DeploymentStatus = "BUILT"
	StatusScheduling DeploymentStatus = "SCHEDULING"
	StatusStarting   DeploymentStatus = "STARTING"
	StatusRunning    DeploymentStatus = "RUNNING"
	StatusFailed     DeploymentStatus = "FAILED"
	StatusStopped    DeploymentStatus = "STOPPED"
	StatusRolledBack DeploymentStatus = "ROLLED_BACK"
)

// IsTerminal returns true if the status represents an end state.
func (s DeploymentStatus) IsTerminal() bool {
	return s == StatusRunning || s == StatusFailed || s == StatusStopped || s == StatusRolledBack
}

// IsInFlight returns true if the deployment was interrupted mid-flight before completion.
func (s DeploymentStatus) IsInFlight() bool {
	return s == StatusQueued || s == StatusBuilding || s == StatusBuilt || s == StatusScheduling || s == StatusStarting
}

// Deployment represents a requested application release.
type Deployment struct {
	ID            string            `json:"id"`
	ProjectID     string            `json:"project_id"`
	Revision      string            `json:"revision"`
	Image         string            `json:"image"`
	ImageDigest   string            `json:"image_digest"`
	DesiredState  string            `json:"desired_state"`
	Status        DeploymentStatus  `json:"status"`
	Stage         string            `json:"stage"`
	InstanceCount   int               `json:"instance_count"`
	DesiredReplicas int               `json:"desired_replicas"`
	Env             map[string]string `json:"env"`
	Labels          map[string]string `json:"labels"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

// Instance represents a single running container instance of a deployment on a worker.
type Instance struct {
	ID           string    `json:"id"`
	DeploymentID string    `json:"deployment_id"`
	WorkerID     string    `json:"worker_id"`
	InstanceKey  string    `json:"instance_key"`
	Status       string    `json:"status"`
	ContainerID  string    `json:"container_id"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}
