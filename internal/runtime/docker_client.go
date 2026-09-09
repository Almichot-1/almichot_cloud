package runtime

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/rs/zerolog"
)

// PortMapping defines host-to-container port mapping.
type PortMapping struct {
	HostPort      int
	ContainerPort int
	Protocol      string
}

// CreateContainerOptions holds parameters for container creation.
type CreateContainerOptions struct {
	InstanceID   string
	DeploymentID string
	Image        string
	Cmd          []string
	Env          []string
	Ports        []PortMapping
	Labels       map[string]string
}

// ContainerDetails holds detailed container inspection information.
type ContainerDetails struct {
	ID        string
	Image     string
	State     string // "running", "exited", "created", "stopped", etc.
	Status    string
	ExitCode  int
	Error     string
	Labels    map[string]string
	CreatedAt time.Time
}

// ContainerSummary holds high-level container info for listings.
type ContainerSummary struct {
	ID         string
	InstanceID string
	Image      string
	State      string
	Status     string
	Labels     map[string]string
	CreatedAt  time.Time
}

// DockerClient is an abstraction interface over container engine operations.
type DockerClient interface {
	CreateContainer(ctx context.Context, opts CreateContainerOptions) (string, error)
	StartContainer(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string, timeout *time.Duration) error
	InspectContainer(ctx context.Context, containerID string) (*ContainerDetails, error)
	ListContainers(ctx context.Context) ([]ContainerSummary, error)
	RemoveContainer(ctx context.Context, containerID string, force bool) error
}

// RealDockerClient implements DockerClient using the official Docker Engine API.
type RealDockerClient struct {
	cli *client.Client
	log zerolog.Logger
}

// NewRealDockerClient creates a new Docker client connected to the local Docker daemon.
func NewRealDockerClient(log zerolog.Logger) (*RealDockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}
	return &RealDockerClient{cli: cli, log: log}, nil
}

// Close closes the underlying Docker client.
func (d *RealDockerClient) Close() error {
	if d.cli != nil {
		return d.cli.Close()
	}
	return nil
}

func (d *RealDockerClient) CreateContainer(ctx context.Context, opts CreateContainerOptions) (string, error) {
	// Ensure image exists locally, otherwise pull
	_, _, err := d.cli.ImageInspectWithRaw(ctx, opts.Image)
	if err != nil {
		d.log.Info().Str("image", opts.Image).Msg("image not found locally, pulling...")
		pullReader, pullErr := d.cli.ImagePull(ctx, opts.Image, image.PullOptions{})
		if pullErr != nil {
			return "", fmt.Errorf("failed to pull image %s: %w", opts.Image, pullErr)
		}
		defer pullReader.Close()
		// Drain output
		_, _ = io.Copy(io.Discard, pullReader)
	}

	// Prepare labels
	labels := make(map[string]string)
	for k, v := range opts.Labels {
		labels[k] = v
	}
	labels["nebula.instance_id"] = opts.InstanceID
	if opts.DeploymentID != "" {
		labels["nebula.deployment_id"] = opts.DeploymentID
	}

	// Prepare port bindings
	exposedPorts := nat.PortSet{}
	portBindings := nat.PortMap{}
	for _, p := range opts.Ports {
		proto := p.Protocol
		if proto == "" {
			proto = "tcp"
		}
		containerPortKey, err := nat.NewPort(proto, fmt.Sprintf("%d", p.ContainerPort))
		if err != nil {
			continue
		}
		exposedPorts[containerPortKey] = struct{}{}
		portBindings[containerPortKey] = []nat.PortBinding{
			{
				HostIP:   "0.0.0.0",
				HostPort: fmt.Sprintf("%d", p.HostPort),
			},
		}
	}

	config := &container.Config{
		Image:        opts.Image,
		Cmd:          opts.Cmd,
		Env:          opts.Env,
		Labels:       labels,
		ExposedPorts: exposedPorts,
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		RestartPolicy: container.RestartPolicy{
			Name: "no",
		},
	}

	containerName := fmt.Sprintf("nebula-%s", opts.InstanceID)
	if workerKey, ok := opts.Labels["nebula.worker_key"]; ok && workerKey != "" {
		containerName = fmt.Sprintf("nebula-%s-%s", workerKey, opts.InstanceID)
	}
	resp, err := d.cli.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
	if err != nil {
		return "", fmt.Errorf("failed to create docker container: %w", err)
	}

	return resp.ID, nil
}

func (d *RealDockerClient) StartContainer(ctx context.Context, containerID string) error {
	if err := d.cli.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container %s: %w", containerID, err)
	}
	return nil
}

func (d *RealDockerClient) StopContainer(ctx context.Context, containerID string, timeout *time.Duration) error {
	stopOpts := container.StopOptions{}
	if timeout != nil {
		seconds := int(timeout.Seconds())
		stopOpts.Timeout = &seconds
	}
	if err := d.cli.ContainerStop(ctx, containerID, stopOpts); err != nil {
		return fmt.Errorf("failed to stop container %s: %w", containerID, err)
	}
	return nil
}

func (d *RealDockerClient) InspectContainer(ctx context.Context, containerID string) (*ContainerDetails, error) {
	inspect, err := d.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect container %s: %w", containerID, err)
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)
	exitCode := 0
	stateStr := "unknown"
	errMsg := ""

	if inspect.State != nil {
		stateStr = inspect.State.Status
		exitCode = inspect.State.ExitCode
		errMsg = inspect.State.Error
	}

	return &ContainerDetails{
		ID:        inspect.ID,
		Image:     inspect.Config.Image,
		State:     stateStr,
		Status:    stateStr,
		ExitCode:  exitCode,
		Error:     errMsg,
		Labels:    inspect.Config.Labels,
		CreatedAt: createdAt,
	}, nil
}

func (d *RealDockerClient) ListContainers(ctx context.Context) ([]ContainerSummary, error) {
	list, err := d.cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	var summaries []ContainerSummary
	for _, c := range list {
		summaries = append(summaries, ContainerSummary{
			ID:         c.ID,
			InstanceID: c.Labels["nebula.instance_id"],
			Image:      c.Image,
			State:      c.State,
			Status:     c.Status,
			Labels:     c.Labels,
			CreatedAt:  time.Unix(c.Created, 0),
		})
	}
	return summaries, nil
}

func (d *RealDockerClient) RemoveContainer(ctx context.Context, containerID string, force bool) error {
	return d.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: force})
}

// MockDockerClient provides a thread-safe in-memory Docker simulation for testing and environments without Docker.
type MockDockerClient struct {
	mu           sync.RWMutex
	containers   map[string]*MockContainer
	counter      int
	FailCreate   error
	FailStart    error
	FailStop     error
	FailInspect  error
	CreateCalls  int
	StartCalls   int
	StopCalls    int
	InspectCalls int
}

// MockContainer represents a simulated container in MockDockerClient.
type MockContainer struct {
	ID           string
	InstanceID   string
	DeploymentID string
	Image        string
	Env          []string
	Labels       map[string]string
	Ports        []PortMapping
	State        string // "created", "running", "stopped", "exited"
	Status       string
	ExitCode     int
	Error        string
	CreatedAt    time.Time
}

// NewMockDockerClient creates a new mock Docker client.
func NewMockDockerClient() *MockDockerClient {
	return &MockDockerClient{
		containers: make(map[string]*MockContainer),
	}
}

func (m *MockDockerClient) CreateContainer(ctx context.Context, opts CreateContainerOptions) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CreateCalls++
	if m.FailCreate != nil {
		return "", m.FailCreate
	}

	m.counter++
	id := fmt.Sprintf("mock-ctr-%d-%s", m.counter, opts.InstanceID)

	labels := make(map[string]string)
	for k, v := range opts.Labels {
		labels[k] = v
	}
	labels["nebula.instance_id"] = opts.InstanceID
	if opts.DeploymentID != "" {
		labels["nebula.deployment_id"] = opts.DeploymentID
	}

	m.containers[id] = &MockContainer{
		ID:           id,
		InstanceID:   opts.InstanceID,
		DeploymentID: opts.DeploymentID,
		Image:        opts.Image,
		Env:          opts.Env,
		Labels:       labels,
		Ports:        opts.Ports,
		State:        "created",
		Status:       "Created",
		CreatedAt:    time.Now().UTC(),
	}

	return id, nil
}

func (m *MockDockerClient) StartContainer(ctx context.Context, containerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.StartCalls++
	if m.FailStart != nil {
		return m.FailStart
	}

	c, ok := m.containers[containerID]
	if !ok {
		return fmt.Errorf("container not found: %s", containerID)
	}

	c.State = "running"
	c.Status = "Up Less than a second"
	return nil
}

func (m *MockDockerClient) StopContainer(ctx context.Context, containerID string, timeout *time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.StopCalls++
	if m.FailStop != nil {
		return m.FailStop
	}

	c, ok := m.containers[containerID]
	if !ok {
		return fmt.Errorf("container not found: %s", containerID)
	}

	c.State = "stopped"
	c.Status = "Exited (0) Less than a second ago"
	c.ExitCode = 0
	return nil
}

func (m *MockDockerClient) InspectContainer(ctx context.Context, containerID string) (*ContainerDetails, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.InspectCalls++
	if m.FailInspect != nil {
		return nil, m.FailInspect
	}

	c, ok := m.containers[containerID]
	if !ok {
		return nil, fmt.Errorf("container not found: %s", containerID)
	}

	return &ContainerDetails{
		ID:        c.ID,
		Image:     c.Image,
		State:     c.State,
		Status:    c.Status,
		ExitCode:  c.ExitCode,
		Error:     c.Error,
		Labels:    c.Labels,
		CreatedAt: c.CreatedAt,
	}, nil
}

func (m *MockDockerClient) ListContainers(ctx context.Context) ([]ContainerSummary, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	summaries := make([]ContainerSummary, 0, len(m.containers))
	for _, c := range m.containers {
		summaries = append(summaries, ContainerSummary{
			ID:         c.ID,
			InstanceID: c.InstanceID,
			Image:      c.Image,
			State:      c.State,
			Status:     c.Status,
			Labels:     c.Labels,
			CreatedAt:  c.CreatedAt,
		})
	}
	return summaries, nil
}

func (m *MockDockerClient) RemoveContainer(ctx context.Context, containerID string, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.containers, containerID)
	return nil
}

// GetMockContainer returns the simulated container state by ID.
func (m *MockDockerClient) GetMockContainer(containerID string) (*MockContainer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.containers[containerID]
	if !ok {
		return nil, false
	}
	// return copy
	clone := *c
	return &clone, true
}
