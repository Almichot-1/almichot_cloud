package deployments

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/secrets"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// WorkerClient abstracts communication with a worker agent.
type WorkerClient interface {
	RunContainer(ctx context.Context, req *proto.RunContainerRequest, opts ...grpc.CallOption) (*proto.RunContainerResponse, error)
	StopContainer(ctx context.Context, req *proto.StopContainerRequest, opts ...grpc.CallOption) (*proto.StopContainerResponse, error)
	ListContainers(ctx context.Context, req *proto.ListContainersRequest, opts ...grpc.CallOption) (*proto.ListContainersResponse, error)
}

// WorkerClientFactory produces or pools WorkerClients.
type WorkerClientFactory interface {
	GetClient(ctx context.Context, worker *workers.Worker) (WorkerClient, error)
}

// GRPCWorkerClientFactory manages real gRPC connections to worker agents.
type GRPCWorkerClientFactory struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func NewGRPCWorkerClientFactory() *GRPCWorkerClientFactory {
	return &GRPCWorkerClientFactory{
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (f *GRPCWorkerClientFactory) GetClient(ctx context.Context, worker *workers.Worker) (WorkerClient, error) {
	addr := worker.Address()
	if addr == "" {
		return nil, fmt.Errorf("worker has empty address")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	conn, ok := f.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to dial worker at %s: %w", addr, err)
		}
		f.conns[addr] = conn
	}

	return proto.NewWorkerServiceClient(conn), nil
}

func (f *GRPCWorkerClientFactory) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, conn := range f.conns {
		_ = conn.Close()
	}
}

// MockWorkerClientFactory is a test double for WorkerClientFactory.
type MockWorkerClientFactory struct {
	mu           sync.Mutex
	Dispatched   []*proto.RunContainerRequest
	Stopped      []*proto.StopContainerRequest
	TargetWorker map[string]string // InstanceKey -> WorkerKey
	Containers   map[string][]*proto.ContainerInfo // WorkerID or WorkerKey -> list of containers
	FailRun      error
	FailStop     error
	FailList     error
}

func NewMockWorkerClientFactory() *MockWorkerClientFactory {
	return &MockWorkerClientFactory{
		TargetWorker: make(map[string]string),
		Containers:   make(map[string][]*proto.ContainerInfo),
	}
}

func (m *MockWorkerClientFactory) GetClient(ctx context.Context, worker *workers.Worker) (WorkerClient, error) {
	return &mockClient{factory: m, worker: worker}, nil
}

// RemoveContainer simulates an external deletion (like docker rm -f)
func (m *MockWorkerClientFactory) RemoveContainer(workerKeyOrID string, containerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	removed := false
	for k, list := range m.Containers {
		var updated []*proto.ContainerInfo
		for _, c := range list {
			if c.ContainerId == containerID || c.InstanceId == containerID {
				removed = true
			} else {
				updated = append(updated, c)
			}
		}
		m.Containers[k] = updated
	}
	return removed
}

// AddContainer simulates an existing or unmanaged/foreign container running on a worker
func (m *MockWorkerClientFactory) AddContainer(workerKeyOrID string, info *proto.ContainerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Containers[workerKeyOrID] = append(m.Containers[workerKeyOrID], info)
}

type mockClient struct {
	factory *MockWorkerClientFactory
	worker  *workers.Worker
}

func (c *mockClient) RunContainer(ctx context.Context, req *proto.RunContainerRequest, opts ...grpc.CallOption) (*proto.RunContainerResponse, error) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.FailRun != nil {
		return nil, c.factory.FailRun
	}

	c.factory.Dispatched = append(c.factory.Dispatched, req)
	c.factory.TargetWorker[req.InstanceId] = c.worker.WorkerKey

	ctrID := fmt.Sprintf("mock-ctr-%s", req.InstanceId)

	// Update simulated containers on this worker
	labels := make(map[string]string)
	for k, v := range req.Labels {
		labels[k] = v
	}
	labels["nebula.instance_id"] = req.InstanceId
	if req.DeploymentId != "" {
		labels["nebula.deployment_id"] = req.DeploymentId
	}

	// Remove any existing with same instance ID
	var updated []*proto.ContainerInfo
	for _, existing := range c.factory.Containers[c.worker.ID] {
		if existing.InstanceId != req.InstanceId {
			updated = append(updated, existing)
		}
	}
	updated = append(updated, &proto.ContainerInfo{
		InstanceId:  req.InstanceId,
		ContainerId: ctrID,
		Image:       req.Image,
		Status:      "running",
		Labels:      labels,
	})
	c.factory.Containers[c.worker.ID] = updated
	c.factory.Containers[c.worker.WorkerKey] = updated

	return &proto.RunContainerResponse{
		InstanceId:  req.InstanceId,
		ContainerId: ctrID,
		Status:      "RUNNING",
	}, nil
}

func (c *mockClient) StopContainer(ctx context.Context, req *proto.StopContainerRequest, opts ...grpc.CallOption) (*proto.StopContainerResponse, error) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.FailStop != nil {
		return nil, c.factory.FailStop
	}

	c.factory.Stopped = append(c.factory.Stopped, req)

	// Remove or mark stopped in simulated worker containers
	for _, key := range []string{c.worker.ID, c.worker.WorkerKey} {
		var updated []*proto.ContainerInfo
		for _, ctr := range c.factory.Containers[key] {
			if ctr.InstanceId == req.InstanceId || ctr.ContainerId == req.InstanceId {
				// removed upon stop
				continue
			}
			updated = append(updated, ctr)
		}
		c.factory.Containers[key] = updated
	}

	return &proto.StopContainerResponse{
		InstanceId: req.InstanceId,
		Success:    true,
	}, nil
}

func (c *mockClient) ListContainers(ctx context.Context, req *proto.ListContainersRequest, opts ...grpc.CallOption) (*proto.ListContainersResponse, error) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.FailList != nil {
		return nil, c.factory.FailList
	}

	seen := make(map[string]bool)
	var list []*proto.ContainerInfo
	for _, k := range []string{c.worker.ID, c.worker.WorkerKey} {
		for _, ctr := range c.factory.Containers[k] {
			id := ctr.ContainerId
			if id == "" {
				id = ctr.InstanceId
			}
			if !seen[id] {
				seen[id] = true
				list = append(list, ctr)
			}
		}
	}

	res := make([]*proto.ContainerInfo, len(list))
	copy(res, list)

	return &proto.ListContainersResponse{
		Containers: res,
	}, nil
}


// PortSpec describes a host-to-container port mapping for a deployed instance.
type PortSpec struct {
	HostPort      int    `json:"host_port"`
	ContainerPort int    `json:"container_port"`
	Protocol      string `json:"protocol"`
}

// CreateDeploymentParams contains arguments to create, build, and schedule a deployment.
type CreateDeploymentParams struct {
	ProjectID     string
	SourcePath    string
	Image         string
	InstanceCount int
	Env           map[string]string
	Labels        map[string]string
	Ports         []PortSpec
}

// Service orchestrates deployment creation, building, worker selection via Scheduler,
// container execution via WorkerClient, and load balancer traffic routing (OW-01, DL-01).
type Service struct {
	depRepo        DeploymentRepository
	instRepo       InstanceRepository
	registry       *workers.Registry
	sched          *scheduler.Scheduler
	clientFactory  WorkerClientFactory
	builder        *build.Orchestrator
	registryClient registry.RegistryClient
	router         *loadbalancer.Router
	crashHook      func(stage DeploymentStatus) error
	projectLocks   sync.Map // projectID -> *sync.Mutex (RACE-01, Gate G-21)
	secretStore    secrets.SecretStore
	redactor       *secrets.Redactor
	log            zerolog.Logger
}

func (s *Service) getProjectLock(projectID string) *sync.Mutex {
	if projectID == "" {
		projectID = "default"
	}
	actual, _ := s.projectLocks.LoadOrStore(projectID, &sync.Mutex{})
	return actual.(*sync.Mutex)
}

func NewService(
	depRepo DeploymentRepository,
	instRepo InstanceRepository,
	workerRegistry *workers.Registry,
	sched *scheduler.Scheduler,
	clientFactory WorkerClientFactory,
	log zerolog.Logger,
) *Service {
	return &Service{
		depRepo:        depRepo,
		instRepo:       instRepo,
		registry:       workerRegistry,
		sched:          sched,
		clientFactory:  clientFactory,
		builder:        build.NewOrchestrator(nil, log),
		registryClient: registry.NewMemoryRegistry(log),
		router:         loadbalancer.NewRouter(log),
		log:            log.With().Str("component", "deployment-service").Logger(),
	}
}

// SetCrashHook attaches a callback that can simulate a crash at specific deployment stages (DL-02, G-16..G-20).
func (s *Service) SetCrashHook(hook func(stage DeploymentStatus) error) {
	s.crashHook = hook
}

// SetSecretStore sets the secret store for envelope-encrypted secret handling (SEC-01..04).
func (s *Service) SetSecretStore(ss secrets.SecretStore) {
	s.secretStore = ss
}

// SetRedactor sets the redactor for masking sensitive strings from logs and events (SEC-03).
func (s *Service) SetRedactor(r *secrets.Redactor) {
	s.redactor = r
}

// SetBuildAndRegistry attaches custom build and registry components.
func (s *Service) SetBuildAndRegistry(builder *build.Orchestrator, regClient registry.RegistryClient, router *loadbalancer.Router) {
	if builder != nil {
		s.builder = builder
	}
	if regClient != nil {
		s.registryClient = regClient
	}
	if router != nil {
		s.router = router
	}
}

// Router returns the load balancer router.
func (s *Service) Router() *loadbalancer.Router {
	return s.router
}

// RegistryClient returns the container image registry client.
func (s *Service) RegistryClient() registry.RegistryClient {
	return s.registryClient
}

// CreateAndDeploy creates a deployment record, builds the source if provided, pushes to registry,
// schedules its instances, dispatches containers to workers, and routes traffic via load balancer (DL-01, OW-01).
func (s *Service) CreateAndDeploy(ctx context.Context, params CreateDeploymentParams) (*Deployment, []*Instance, error) {
	if params.Image == "" && params.SourcePath == "" {
		return nil, nil, fmt.Errorf("either image or source_path must be specified")
	}
	if params.InstanceCount <= 0 {
		params.InstanceCount = 1
	}

	// Serialize concurrent deployments targeting the same project (RACE-01, Gate G-21).
	// Different projects execute concurrently without contention (DL-08).
	lock := s.getProjectLock(params.ProjectID)
	lock.Lock()
	defer lock.Unlock()

	depID := uuid.New().String()
	dep := &Deployment{
		ID:            depID,
		ProjectID:     params.ProjectID,
		Image:         params.Image,
		DesiredState:  "RUNNING",
		Status:        StatusQueued,
		Stage:         "QUEUED",
		InstanceCount: params.InstanceCount,
		Env:           params.Env,
		Labels:        params.Labels,
	}

	// 1. Initial State Persisted: QUEUED (DL-02, G-16)
	if err := s.depRepo.Create(ctx, dep); err != nil {
		return nil, nil, fmt.Errorf("failed to create deployment record: %w", err)
	}

	// Snapshot project secrets for this deployment (SEC-01..04)
	if s.secretStore != nil && params.ProjectID != "" {
		_ = s.secretStore.SnapshotForDeployment(ctx, params.ProjectID, depID)
		if secs, err := s.secretStore.GetSecretsForDeployment(ctx, depID); err == nil && s.redactor != nil {
			for _, val := range secs {
				s.redactor.Register(val)
			}
		}
	}

	if s.crashHook != nil {
		if err := s.crashHook(StatusQueued); err != nil {
			return dep, nil, err
		}
	}

	// 2. Build from source if source path is provided (BLD-01..08, G-17, DL-04)
	var buildRes *build.BuildResult
	if params.SourcePath != "" {
		dep.Status = StatusBuilding
		dep.Stage = "BUILDING"
		_ = s.depRepo.UpdateStatus(ctx, depID, StatusBuilding, "BUILDING")

		if s.crashHook != nil {
			if err := s.crashHook(StatusBuilding); err != nil {
				return dep, nil, err
			}
		}

		imageTag := fmt.Sprintf("nebula/%s:%s", params.ProjectID, depID[:8])
		var err error
		buildRes, err = s.builder.BuildFromSource(ctx, params.ProjectID, params.SourcePath, imageTag, params.Env)
		if err != nil {
			dep.Status = StatusFailed
			dep.Stage = "BUILD_FAILED"
			_ = s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "BUILD_FAILED")
			return dep, nil, fmt.Errorf("build failed: %w", err)
		}

		// Push to registry with cryptographic digest verification (BLD-07, BLD-08)
		verifiedDigest, err := s.registryClient.Push(ctx, buildRes.ImageTag, buildRes.ImageData, buildRes.Digest)
		if err != nil {
			dep.Status = StatusFailed
			dep.Stage = "REGISTRY_PUSH_FAILED"
			_ = s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "REGISTRY_PUSH_FAILED")
			return dep, nil, fmt.Errorf("registry push rejected: %w", err)
		}

		dep.Image = buildRes.ImageTag
		dep.ImageDigest = verifiedDigest
		_ = s.depRepo.UpdateImage(ctx, depID, dep.Image, dep.ImageDigest)
	}

	// 3. Transition to SCHEDULING / BUILT (DL-02, G-18)
	dep.Status = StatusScheduling
	dep.Stage = "SCHEDULING"
	_ = s.depRepo.UpdateStatus(ctx, depID, StatusScheduling, "SCHEDULING")

	if s.crashHook != nil {
		if err := s.crashHook(StatusScheduling); err != nil {
			return dep, nil, err
		}
	}

	var createdInstances []*Instance

	// Determine container ports to expose (OW-01): explicit overrides win, otherwise
	// derive from EXPOSE in the built Dockerfile and assign free host ports.
	containerPorts := make([]PortSpec, 0, len(params.Ports))
	for _, p := range params.Ports {
		sp := p
		if sp.Protocol == "" {
			sp.Protocol = "tcp"
		}
		containerPorts = append(containerPorts, sp)
	}
	if len(containerPorts) == 0 && buildRes != nil {
		for _, cp := range buildRes.Ports {
			containerPorts = append(containerPorts, PortSpec{ContainerPort: cp, Protocol: "tcp"})
		}
	}
	for i := range containerPorts {
		if containerPorts[i].HostPort == 0 {
			containerPorts[i].HostPort = freePort()
		}
	}

	// 4. Transition to STARTING before container creation begins (DL-02, G-19)
	dep.Status = StatusStarting
	dep.Stage = "STARTING"
	_ = s.depRepo.UpdateStatus(ctx, depID, StatusStarting, "STARTING")

	if s.crashHook != nil {
		if err := s.crashHook(StatusStarting); err != nil {
			return dep, nil, err
		}
	}

	// 2. Schedule and run instances
	for i := 0; i < params.InstanceCount; i++ {
		instID := uuid.New().String()
		instKey := fmt.Sprintf("inst-%s-%d", depID[:8], i)

		inst := &Instance{
			ID:           instID,
			DeploymentID: depID,
			InstanceKey:  instKey,
			Status:       "PENDING",
		}

		if err := s.instRepo.Create(ctx, inst); err != nil {
			_ = s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "FAILED")
			dep.Status = StatusFailed
			dep.Stage = "FAILED"
			return dep, nil, fmt.Errorf("failed to create instance record: %w", err)
		}

		// Select worker via Scheduler
		targetWorker, err := s.sched.SelectWorker(ctx, scheduler.WorkloadRequirement{
			DeploymentID:     depID,
			RequiredCapacity: 1,
			RequiredLabels:   params.Labels,
		})
		if err != nil {
			inst.Status = "FAILED"
			_ = s.instRepo.Update(ctx, inst)
			_ = s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "FAILED")
			dep.Status = StatusFailed
			dep.Stage = "FAILED"
			return dep, nil, fmt.Errorf("scheduler failed for instance %s: %w", instKey, err)
		}

		// Dispatch RunContainer to worker
		client, err := s.clientFactory.GetClient(ctx, targetWorker)
		if err != nil {
			inst.Status = "FAILED"
			_ = s.instRepo.Update(ctx, inst)
			_ = s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "FAILED")
			dep.Status = StatusFailed
			dep.Stage = "FAILED"
			return dep, nil, fmt.Errorf("failed to connect to worker %s: %w", targetWorker.WorkerKey, err)
		}

		ports := make([]*proto.PortMapping, 0, len(containerPorts))
		for _, p := range containerPorts {
			ports = append(ports, &proto.PortMapping{
				HostPort:      int32(p.HostPort),
				ContainerPort: int32(p.ContainerPort),
				Protocol:      p.Protocol,
			})
		}

		// Inject secrets at RunContainer time only (SEC-02)
		runtimeEnv := params.Env
		if s.secretStore != nil {
			if injected, err := secrets.InjectSecrets(ctx, s.secretStore, depID, params.Env); err == nil {
				runtimeEnv = injected
			}
		}

		runResp, err := client.RunContainer(ctx, &proto.RunContainerRequest{
			InstanceId:   instKey,
			DeploymentId: depID,
			Image:        dep.Image,
			Env:          runtimeEnv,
			Labels:       params.Labels,
			Ports:        ports,
		})
		if err != nil || (runResp != nil && runResp.Error != "") {
			inst.Status = "FAILED"
			_ = s.instRepo.Update(ctx, inst)
			_ = s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "FAILED")
			dep.Status = StatusFailed
			dep.Stage = "FAILED"
			if err != nil {
				return dep, nil, fmt.Errorf("worker RunContainer RPC failed: %w", err)
			}
			return dep, nil, fmt.Errorf("worker RunContainer error: %s", runResp.Error)
		}

		// Update instance with assigned worker and container ID
		inst.WorkerID = targetWorker.ID
		inst.ContainerID = runResp.ContainerId
		inst.Status = "RUNNING"
		if err := s.instRepo.Update(ctx, inst); err != nil {
			s.log.Error().Err(err).Str("instance_id", inst.ID).Msg("failed to update instance record")
		}

		// Register target in load balancer (OW-01)
		if s.router != nil && params.ProjectID != "" {
			host := targetWorker.IPAddress
			if host == "" {
				host = "localhost"
			}
			appPort := 8080
			if len(containerPorts) > 0 && containerPorts[0].HostPort > 0 {
				appPort = containerPorts[0].HostPort
			}
			targetURL := fmt.Sprintf("http://%s:%d", host, appPort)
			_ = s.router.RegisterTarget(params.ProjectID, inst.ID, targetURL)
		}

		// Increment active workload on worker
		s.registry.UpdateWorkload(targetWorker.WorkerKey, 1)

		createdInstances = append(createdInstances, inst)
	}

	// Update deployment status to RUNNING (DL-01, G-05)
	dep.Status = StatusRunning
	dep.Stage = "RUNNING"
	_ = s.depRepo.UpdateStatus(ctx, depID, StatusRunning, "RUNNING")

	if s.crashHook != nil {
		if err := s.crashHook(StatusRunning); err != nil {
			return dep, createdInstances, err
		}
	}

	s.log.Info().
		Str("deployment_id", depID).
		Str("image", dep.Image).
		Str("digest", dep.ImageDigest).
		Int("instances", len(createdInstances)).
		Msg("deployment successfully built, scheduled, and running (DL-01, G-05)")

	return dep, createdInstances, nil
}

// GetDeployment retrieves a deployment and its instances.
func (s *Service) GetDeployment(ctx context.Context, id string) (*Deployment, []*Instance, error) {
	dep, err := s.depRepo.GetByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	instances, err := s.instRepo.ListByDeployment(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	return dep, instances, nil
}

// ListDeployments returns deployments, optionally filtered by project ID.
func (s *Service) ListDeployments(ctx context.Context, projectID string) ([]*Deployment, error) {
	return s.depRepo.List(ctx, projectID)
}

// freePort allocates a random free TCP port on the host for port publishing.
func freePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0
	}
	defer l.Close()
	if addr, ok := l.Addr().(*net.TCPAddr); ok {
		return addr.Port
	}
	return 0
}
