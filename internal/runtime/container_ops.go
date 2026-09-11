package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// RunOptions defines options for running an instance container.
type RunOptions struct {
	InstanceID    string
	DeploymentID  string
	Image         string
	Cmd           []string
	Env           []string
	Ports         []PortMapping
	Labels        map[string]string
	CPULimit      float64
	MemoryLimitMB int64
}

// RunResult represents the result of a RunContainer operation.
type RunResult struct {
	InstanceID  string
	ContainerID string
	Status      string
	Error       string
	IsDuplicate bool
}

// StopResult represents the outcome of a StopContainer operation.
type StopResult struct {
	InstanceID string
	Success    bool
	Error      string
}

// StatusResult represents the inspected status of an instance.
type StatusResult struct {
	InstanceID  string
	ContainerID string
	Status      string
	ExitCode    int
	Error       string
}

// ImageVerifier validates container image cryptographic signatures before execution (Gate G-28, §21.3).
type ImageVerifier interface {
	Verify(ctx context.Context, image, digest, signature string) error
}

// ContainerOps orchestrates container lifecycle operations across the Docker client
// and the local instance tracker.
type ContainerOps struct {
	client   DockerClient
	tracker  *InstanceTracker
	verifier ImageVerifier
	log      zerolog.Logger
}

// NewContainerOps creates a new ContainerOps instance.
func NewContainerOps(client DockerClient, tracker *InstanceTracker, log zerolog.Logger) *ContainerOps {
	return &ContainerOps{
		client:  client,
		tracker: tracker,
		log:     log,
	}
}

// SetVerifier sets the cryptographic image verifier for supply-chain integrity enforcement (G-28).
func (c *ContainerOps) SetVerifier(v ImageVerifier) {
	c.verifier = v
}

// Client returns the underlying DockerClient.
func (c *ContainerOps) Client() DockerClient {
	return c.client
}

// Tracker returns the local InstanceTracker.
func (c *ContainerOps) Tracker() *InstanceTracker {
	return c.tracker
}

// RunContainer runs a container for the given instance idempotently.
// If the instance is already running, this is a no-op that returns the existing container (G-09).
func (c *ContainerOps) RunContainer(ctx context.Context, opts RunOptions) (*RunResult, error) {
	if opts.InstanceID == "" {
		return nil, fmt.Errorf("instance_id is required")
	}

	unlock := c.tracker.LockInstance(opts.InstanceID)
	defer unlock()

	// Check if already running or starting
	if rec, exists := c.tracker.Get(opts.InstanceID); exists {
		if rec.State == StateRunning || rec.State == StateStarting {
			c.log.Info().
				Str("instance_id", opts.InstanceID).
				Str("container_id", rec.ContainerID).
				Msg("instance is already running; no-op duplicate invocation")
			return &RunResult{
				InstanceID:  opts.InstanceID,
				ContainerID: rec.ContainerID,
				Status:      string(rec.State),
				IsDuplicate: true,
			}, nil
		}
	}

	// Verify cryptographic signature if verifier is configured (Gate G-28, §21.3).
	// Reject unsigned or signature-mismatched images before docker run.
	if c.verifier != nil {
		var digest, sig string
		if opts.Labels != nil {
			digest = opts.Labels["nebula.image_digest"]
			sig = opts.Labels["nebula.signature"]
		}
		if err := c.verifier.Verify(ctx, opts.Image, digest, sig); err != nil {
			rec := &InstanceRecord{
				InstanceID:   opts.InstanceID,
				DeploymentID: opts.DeploymentID,
				Image:        opts.Image,
				State:        StateFailed,
				LastError:    fmt.Sprintf("signature verification rejected: %v", err),
				Labels:       opts.Labels,
			}
			c.tracker.Set(rec)

			c.log.Error().
				Err(err).
				Str("instance_id", opts.InstanceID).
				Str("image", opts.Image).
				Str("digest", digest).
				Msg("image signature verification failed; rejected before container creation (G-28)")

			return &RunResult{
				InstanceID: opts.InstanceID,
				Status:     string(StateFailed),
				Error:      rec.LastError,
			}, err
		}
	}

	// Mark as starting
	record := &InstanceRecord{
		InstanceID:   opts.InstanceID,
		DeploymentID: opts.DeploymentID,
		Image:        opts.Image,
		State:        StateStarting,
		Labels:       opts.Labels,
	}
	c.tracker.Set(record)

	// Create container via Docker client
	createOpts := CreateContainerOptions{
		InstanceID:    opts.InstanceID,
		DeploymentID:  opts.DeploymentID,
		Image:         opts.Image,
		Cmd:           opts.Cmd,
		Env:           opts.Env,
		Ports:         opts.Ports,
		Labels:        opts.Labels,
		CPULimit:      opts.CPULimit,
		MemoryLimitMB: opts.MemoryLimitMB,
	}

	containerID, err := c.client.CreateContainer(ctx, createOpts)
	if err != nil {
		record.State = StateFailed
		errLower := strings.ToLower(err.Error())
		if strings.Contains(errLower, "unauthorized") || strings.Contains(errLower, "401") || strings.Contains(errLower, "authentication required") || strings.Contains(errLower, "invalid credentials") || strings.Contains(errLower, "token expired") {
			record.LastError = fmt.Sprintf("REGISTRY_AUTH_FAILED: worker registry credentials rejected or expired: %v", err)
		} else {
			record.LastError = fmt.Sprintf("docker create failed: %v", err)
		}
		c.tracker.Set(record)

		c.log.Error().
			Err(err).
			Str("instance_id", opts.InstanceID).
			Msg("failed to create container")

		return &RunResult{
			InstanceID: opts.InstanceID,
			Status:     string(StateFailed),
			Error:      record.LastError,
		}, fmt.Errorf("%s", record.LastError)
	}

	record.ContainerID = containerID

	// Start container
	if err := c.client.StartContainer(ctx, containerID); err != nil {
		record.State = StateFailed
		record.LastError = fmt.Sprintf("docker start failed: %v", err)
		c.tracker.Set(record)

		c.log.Error().
			Err(err).
			Str("instance_id", opts.InstanceID).
			Str("container_id", containerID).
			Msg("failed to start container")

		return &RunResult{
			InstanceID:  opts.InstanceID,
			ContainerID: containerID,
			Status:      string(StateFailed),
			Error:       record.LastError,
		}, err
	}

	record.State = StateRunning
	record.LastError = ""
	c.tracker.Set(record)

	c.log.Info().
		Str("instance_id", opts.InstanceID).
		Str("container_id", containerID).
		Str("image", opts.Image).
		Msg("container successfully started")

	return &RunResult{
		InstanceID:  opts.InstanceID,
		ContainerID: containerID,
		Status:      string(StateRunning),
		IsDuplicate: false,
	}, nil
}

// StopContainer stops an instance's container and records stopped status.
func (c *ContainerOps) StopContainer(ctx context.Context, instanceID string, timeout time.Duration) (*StopResult, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("instance_id is required")
	}

	unlock := c.tracker.LockInstance(instanceID)
	defer unlock()

	rec, exists := c.tracker.Get(instanceID)
	if !exists {
		return &StopResult{
			InstanceID: instanceID,
			Success:    true,
		}, nil
	}

	if rec.State == StateStopped {
		return &StopResult{
			InstanceID: instanceID,
			Success:    true,
		}, nil
	}

	rec.State = StateStopping
	c.tracker.Set(rec)

	var stopTimeout *time.Duration
	if timeout > 0 {
		stopTimeout = &timeout
	}

	if rec.ContainerID != "" {
		if err := c.client.StopContainer(ctx, rec.ContainerID, stopTimeout); err != nil {
			rec.LastError = fmt.Sprintf("docker stop failed: %v", err)
			c.tracker.Set(rec)

			c.log.Error().
				Err(err).
				Str("instance_id", instanceID).
				Str("container_id", rec.ContainerID).
				Msg("failed to stop container")

			return &StopResult{
				InstanceID: instanceID,
				Success:    false,
				Error:      rec.LastError,
			}, err
		}
	}

	rec.State = StateStopped
	rec.LastError = ""
	c.tracker.Set(rec)

	c.log.Info().
		Str("instance_id", instanceID).
		Str("container_id", rec.ContainerID).
		Msg("container stopped")

	return &StopResult{
		InstanceID: instanceID,
		Success:    true,
	}, nil
}

// GetContainerStatus retrieves the current status of an instance.
func (c *ContainerOps) GetContainerStatus(ctx context.Context, instanceID string) (*StatusResult, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("instance_id is required")
	}

	rec, exists := c.tracker.Get(instanceID)
	if !exists {
		return &StatusResult{
			InstanceID: instanceID,
			Status:     "NOT_FOUND",
		}, nil
	}

	if rec.ContainerID != "" {
		details, err := c.client.InspectContainer(ctx, rec.ContainerID)
		if err == nil {
			return &StatusResult{
				InstanceID:  instanceID,
				ContainerID: rec.ContainerID,
				Status:      details.State,
				ExitCode:    details.ExitCode,
				Error:       details.Error,
			}, nil
		}
	}

	return &StatusResult{
		InstanceID:  instanceID,
		ContainerID: rec.ContainerID,
		Status:      string(rec.State),
		ExitCode:    rec.ExitCode,
		Error:       rec.LastError,
	}, nil
}

// ListContainers returns all tracked instance records.
func (c *ContainerOps) ListContainers(ctx context.Context) ([]*InstanceRecord, error) {
	return c.tracker.List(), nil
}
