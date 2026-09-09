package discovery

import "time"

// EndpointStatus represents health status of a service endpoint.
type EndpointStatus string

const (
	EndpointHealthy   EndpointStatus = "HEALTHY"
	EndpointUnhealthy EndpointStatus = "UNHEALTHY"
)

// Endpoint represents an active container workload endpoint available for routing.
type Endpoint struct {
	InstanceID   string         `json:"instance_id"`
	DeploymentID string         `json:"deployment_id"`
	ProjectID    string         `json:"project_id"`
	WorkerID     string         `json:"worker_id"`
	Address      string         `json:"address"` // e.g. "http://10.0.0.1:8080"
	Status       EndpointStatus `json:"status"`
	UpdatedAt    time.Time      `json:"updated_at"`
}
