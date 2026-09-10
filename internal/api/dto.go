package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/workers"
)

// ErrorResponse is the standard error envelope.
type ErrorResponse struct {
	Error        string `json:"error"`
	DeploymentID string `json:"deployment_id,omitempty"`
	Status       string `json:"status,omitempty"`
}

// ProjectCreateRequest is the body for creating a project.
type ProjectCreateRequest struct {
	Name          string `json:"name"`
	Description   string `json:"description"`
	RepoURL       string `json:"repo_url"`
	DefaultBranch string `json:"default_branch,omitempty"`
	RootDir       string `json:"root_dir,omitempty"`
	WebhookSecret string `json:"webhook_secret,omitempty"`
}

// ProjectResponse is the API representation of a project.
type ProjectResponse struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Description     string `json:"description"`
	RepoURL         string `json:"repo_url"`
	DefaultBranch   string `json:"default_branch,omitempty"`
	RootDir         string `json:"root_dir,omitempty"`
	DesiredReplicas int    `json:"desired_replicas,omitempty"`
	MinReplicas     int    `json:"min_replicas,omitempty"`
	MaxReplicas     int    `json:"max_replicas,omitempty"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

// DeploymentCreateRequest is the body for triggering a deployment.
type DeploymentCreateRequest struct {
	Image         string                 `json:"image"`
	SourcePath    string                 `json:"source_path"`
	Revision      string                 `json:"revision"`
	InstanceCount int                    `json:"instance_count"`
	Env           map[string]string      `json:"env"`
	Labels        map[string]string      `json:"labels"`
	Ports         []deployments.PortSpec `json:"ports"`
}

// InstanceResponse is the API representation of a deployment instance.
type InstanceResponse struct {
	ID           string `json:"id"`
	DeploymentID string `json:"deployment_id"`
	WorkerID     string `json:"worker_id"`
	InstanceKey  string `json:"instance_key"`
	Status       string `json:"status"`
	ContainerID  string `json:"container_id"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// DeploymentResponse is the API representation of a deployment.
type DeploymentResponse struct {
	ID              string             `json:"id"`
	ProjectID       string             `json:"project_id"`
	Revision        string             `json:"revision"`
	Image           string             `json:"image"`
	ImageDigest     string             `json:"image_digest"`
	DesiredState    string             `json:"desired_state"`
	Status          string             `json:"status"`
	Stage           string             `json:"stage"`
	InstanceCount   int                `json:"instance_count"`
	DesiredReplicas int                `json:"desired_replicas,omitempty"`
	Instances       []InstanceResponse `json:"instances,omitempty"`
	CreatedAt       string             `json:"created_at"`
	UpdatedAt       string             `json:"updated_at"`
}

// WorkerResponse is the API representation of a registered worker.
type WorkerResponse struct {
	ID              string            `json:"id"`
	WorkerKey       string            `json:"worker_key"`
	Hostname        string            `json:"hostname"`
	IPAddress       string            `json:"ip_address"`
	GRPCPort        int               `json:"grpc_port"`
	Capacity        int               `json:"capacity"`
	ActiveWorkloads int               `json:"active_workloads"`
	State           string            `json:"state"`
	Health          string            `json:"health"`
	Schedulable     bool              `json:"schedulable"`
	Labels          map[string]string `json:"labels"`
}

func newProjectResponse(p *projects.Project) ProjectResponse {
	return ProjectResponse{
		ID:              p.ID,
		Name:            p.Name,
		Description:     p.Description,
		RepoURL:         p.RepoURL,
		DefaultBranch:   p.DefaultBranch,
		RootDir:         p.RootDir,
		DesiredReplicas: p.DesiredReplicas,
		MinReplicas:     p.MinReplicas,
		MaxReplicas:     p.MaxReplicas,
		CreatedAt:       p.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		UpdatedAt:       p.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

func newInstanceResponse(inst *deployments.Instance) InstanceResponse {
	return InstanceResponse{
		ID:           inst.ID,
		DeploymentID: inst.DeploymentID,
		WorkerID:     inst.WorkerID,
		InstanceKey:  inst.InstanceKey,
		Status:       inst.Status,
		ContainerID:  inst.ContainerID,
		CreatedAt:    inst.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		UpdatedAt:    inst.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

func newDeploymentResponse(dep *deployments.Deployment, instances []*deployments.Instance) DeploymentResponse {
	resp := DeploymentResponse{
		ID:              dep.ID,
		ProjectID:       dep.ProjectID,
		Revision:        dep.Revision,
		Image:           dep.Image,
		ImageDigest:     dep.ImageDigest,
		DesiredState:    dep.DesiredState,
		Status:          string(dep.Status),
		Stage:           dep.Stage,
		InstanceCount:   dep.InstanceCount,
		DesiredReplicas: dep.DesiredReplicas,
		CreatedAt:       dep.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		UpdatedAt:       dep.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
	for _, inst := range instances {
		resp.Instances = append(resp.Instances, newInstanceResponse(inst))
	}
	return resp
}

func newWorkerResponse(w *workers.Worker) WorkerResponse {
	return WorkerResponse{
		ID:              w.ID,
		WorkerKey:       w.WorkerKey,
		Hostname:        w.Hostname,
		IPAddress:       w.IPAddress,
		GRPCPort:        w.GRPCPort,
		Capacity:        w.Capacity,
		ActiveWorkloads: w.ActiveWorkloads,
		State:           string(w.State),
		Health:          string(w.Health),
		Schedulable:     w.Schedulable,
		Labels:          w.Labels,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return err
	}
	return nil
}

func notFound(w http.ResponseWriter, err error) {
	if errors.Is(err, projects.ErrProjectNotFound) ||
		errors.Is(err, deployments.ErrDeploymentNotFound) ||
		errors.Is(err, deployments.ErrInstanceNotFound) {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeErr(w, http.StatusBadRequest, err.Error())
}
