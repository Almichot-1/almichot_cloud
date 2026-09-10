package deployments

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/ha"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/observability"
	"github.com/nebula/nebula/internal/registry"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/secrets"
	"github.com/nebula/nebula/internal/transport"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
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
		conn, err = transport.NewClientConn(addr)
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
	TargetWorker map[string]string                 // InstanceKey -> WorkerKey
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
	releaseRepo    ReleaseRepository
	eventRepo      EventRepository
	registry       *workers.Registry
	sched          *scheduler.Scheduler
	clientFactory  WorkerClientFactory
	builder        *build.Orchestrator
	registryClient registry.RegistryClient
	signer         registry.ImageSigner
	router         *loadbalancer.Router
	crashHook      func(stage DeploymentStatus) error
	projectLocks   sync.Map // projectID -> *sync.Mutex (RACE-01, Gate G-21)
	secretStore    secrets.SecretStore
	redactor       *secrets.Redactor
	haElector      *ha.Elector
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
		releaseRepo:    NewMemoryReleaseRepository(),
		eventRepo:      NewMemoryEventRepository(),
		registry:       workerRegistry,
		sched:          sched,
		clientFactory:  clientFactory,
		builder:        build.NewOrchestrator(nil, log),
		registryClient: registry.NewMemoryRegistry(log),
		router:         loadbalancer.NewRouter(log),
		log:            log.With().Str("component", "deployment-service").Logger(),
	}
}

// SetHAElector configures the high-availability leader election coordinator (Phase 16, Gate G-44).
func (s *Service) SetHAElector(e *ha.Elector) {
	s.haElector = e
}

// HAElector returns the configured high-availability leader election coordinator.
func (s *Service) HAElector() *ha.Elector {
	return s.haElector
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

// SetReleaseRepo sets the durable release repository (§10.1, §19.1).
func (s *Service) SetReleaseRepo(repo ReleaseRepository) {
	s.releaseRepo = repo
}

// SetEventRepo sets the operational event repository (§19.1, §22, §27.2).
func (s *Service) SetEventRepo(repo EventRepository) {
	s.eventRepo = repo
}

// EventRepo returns the operational event repository.
func (s *Service) EventRepo() EventRepository {
	return s.eventRepo
}

// SetSigner sets the cryptographic image signer (§21.3).
func (s *Service) SetSigner(signer registry.ImageSigner) {
	s.signer = signer
}

// ReleaseRepo returns the configured release repository.
func (s *Service) ReleaseRepo() ReleaseRepository {
	return s.releaseRepo
}

// Signer returns the configured cryptographic image signer.
func (s *Service) Signer() registry.ImageSigner {
	return s.signer
}

// Router returns the load balancer router.
func (s *Service) Router() *loadbalancer.Router {
	return s.router
}

// RegistryClient returns the container image registry client.
func (s *Service) RegistryClient() registry.RegistryClient {
	return s.registryClient
}

// DepRepo returns the configured deployment repository.
func (s *Service) DepRepo() DeploymentRepository {
	return s.depRepo
}

// InstRepo returns the configured instance repository.
func (s *Service) InstRepo() InstanceRepository {
	return s.instRepo
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

	// Distributed Tracing: API leg (§22, Gate G-39)
	ctx, _ = observability.EnsureTraceID(ctx)
	ctx, apiSpan := observability.StartSpan(ctx, "api.create_deployment")
	defer apiSpan.End()
	startDeployTime := time.Now()

	// HA Fencing check (Phase 16, Gate G-44):
	// Non-leader or standby instances cannot issue scheduling decisions or mutate deployment states.
	if s.haElector != nil {
		if _, err := s.haElector.RecordDecision(); err != nil {
			return nil, nil, err
		}
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

	if s.eventRepo != nil {
		_ = s.eventRepo.Create(ctx, &Event{
			ProjectID:    dep.ProjectID,
			DeploymentID: dep.ID,
			EventType:    "DEPLOYMENT_QUEUED",
			Message:      fmt.Sprintf("Deployment queued for project %s with %d desired replicas", dep.ProjectID, dep.InstanceCount),
		})
	}

	// Snapshot project secrets for this deployment (SEC-01..04)
	if s.secretStore != nil && params.ProjectID != "" {
		if err := s.secretStore.SnapshotForDeployment(ctx, params.ProjectID, depID); err != nil {
			s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to snapshot secrets for deployment")
		}
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
		if err := s.depRepo.UpdateStatus(ctx, depID, StatusBuilding, "BUILDING"); err != nil {
			s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to update deployment status to BUILDING")
		}

		if s.crashHook != nil {
			if err := s.crashHook(StatusBuilding); err != nil {
				return dep, nil, err
			}
		}

		imageTag := fmt.Sprintf("nebula/%s:%s", params.ProjectID, depID[:8])
		var err error
		buildCtx, buildSpan := observability.StartSpan(ctx, "build.orchestrate")
		buildRes, err = s.builder.BuildFromSource(buildCtx, params.ProjectID, params.SourcePath, imageTag, params.Env)
		buildSpan.End()
		if err != nil {
			dep.Status = StatusFailed
			dep.Stage = "BUILD_FAILED"
			if uErr := s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "BUILD_FAILED"); uErr != nil {
				s.log.Error().Err(uErr).Str("deployment_id", depID).Msg("failed to update deployment status to BUILD_FAILED")
			}
			observability.MetricDeploymentStatusTotal.Inc(map[string]string{
				"status":     string(StatusFailed),
				"project_id": dep.ProjectID,
			})
			return dep, nil, fmt.Errorf("build failed: %w", err)
		}

		// Push to registry with cryptographic digest verification (BLD-07, BLD-08)
		verifiedDigest, err := s.registryClient.Push(ctx, buildRes.ImageTag, buildRes.ImageData, buildRes.Digest)
		if err != nil {
			dep.Status = StatusFailed
			failStage := "REGISTRY_PUSH_FAILED"
			if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
				failStage = "REGISTRY_UNAVAILABLE"
			}
			dep.Stage = failStage
			if uErr := s.depRepo.UpdateStatus(ctx, depID, StatusFailed, failStage); uErr != nil {
				s.log.Error().Err(uErr).Str("deployment_id", depID).Msg("failed to update deployment status to " + failStage)
			}
			return dep, nil, fmt.Errorf("registry push rejected: %w", err)
		}

		dep.Image = buildRes.ImageTag
		dep.ImageDigest = verifiedDigest
		if err := s.depRepo.UpdateImage(ctx, depID, dep.Image, dep.ImageDigest); err != nil {
			s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to update deployment image in repo")
		}

		// Transition to BUILT state (§8.2, Phase 10)
		dep.Status = StatusBuilt
		dep.Stage = "BUILT"
		if err := s.depRepo.UpdateStatus(ctx, depID, StatusBuilt, "BUILT"); err != nil {
			s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to update deployment status to BUILT")
		}

		if s.crashHook != nil {
			if err := s.crashHook(StatusBuilt); err != nil {
				return dep, nil, err
			}
		}
	} else if params.Image != "" {
		// Verify registry availability and resolve image digest if registered (§10.1, G-27)
		if s.registryClient != nil {
			digest, err := s.registryClient.GetDigest(ctx, params.Image)
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
					dep.Status = StatusFailed
					dep.Stage = "REGISTRY_UNAVAILABLE"
					if uErr := s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "REGISTRY_UNAVAILABLE"); uErr != nil {
						s.log.Error().Err(uErr).Str("deployment_id", depID).Msg("failed to update deployment status to REGISTRY_UNAVAILABLE")
					}
					return dep, nil, fmt.Errorf("registry unavailable: %w", err)
				}
			} else {
				dep.ImageDigest = digest
				if err := s.depRepo.UpdateImage(ctx, depID, dep.Image, dep.ImageDigest); err != nil {
					s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to update deployment image in repo")
				}
			}
		}
	}

	// Cryptographic Image Signing & Release Recording (§10.1, §19.1, §21.3)
	var signature string
	if s.signer != nil && dep.ImageDigest != "" {
		sig, err := s.signer.Sign(ctx, dep.ImageDigest)
		if err != nil {
			s.log.Error().Err(err).Str("digest", dep.ImageDigest).Msg("failed to sign image digest")
		} else {
			signature = sig
		}
	}

	if s.releaseRepo != nil && dep.Image != "" {
		releaseVer := depID[:8]
		rel := &Release{
			ID:           uuid.New().String(),
			ProjectID:    params.ProjectID,
			DeploymentID: depID,
			Version:      releaseVer,
			ImageRef:     dep.Image,
			ImageDigest:  dep.ImageDigest,
			Signature:    signature,
			CreatedAt:    time.Now().UTC(),
		}
		if err := s.releaseRepo.Create(ctx, rel); err != nil {
			s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to record release in storage")
		}
	}

	var buildPorts []int
	if buildRes != nil {
		buildPorts = buildRes.Ports
	}

	createdInstances, err := s.scheduleAndDispatchInstances(ctx, dep, params.Ports, buildPorts, dep.ImageDigest, signature)
	if err != nil {
		return dep, createdInstances, err
	}

	observability.MetricDeploymentDurationSeconds.Observe(map[string]string{
		"project_id": dep.ProjectID,
		"status":     string(dep.Status),
	}, time.Since(startDeployTime).Seconds())

	s.log.Info().
		Str("deployment_id", depID).
		Str("image", dep.Image).
		Str("digest", dep.ImageDigest).
		Int("instances", len(createdInstances)).
		Msg("deployment successfully built, scheduled, and running (DL-01, G-05)")

	return dep, createdInstances, nil
}

// scheduleAndDispatchInstances executes the unified scheduler and worker container dispatch pipeline (DL-01, OW-01, §27.2).
func (s *Service) scheduleAndDispatchInstances(
	ctx context.Context,
	dep *Deployment,
	requestedPorts []PortSpec,
	buildPorts []int,
	digest string,
	signature string,
) ([]*Instance, error) {
	depID := dep.ID

	// 1. Transition to SCHEDULING (DL-02, G-18)
	dep.Status = StatusScheduling
	dep.Stage = "SCHEDULING"
	if err := s.depRepo.UpdateStatus(ctx, depID, StatusScheduling, "SCHEDULING"); err != nil {
		s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to update deployment status to SCHEDULING")
	}

	if s.crashHook != nil {
		if err := s.crashHook(StatusScheduling); err != nil {
			return nil, err
		}
	}

	var createdInstances []*Instance

	// Determine container ports to expose (OW-01): explicit overrides win, otherwise
	// derive from EXPOSE in Dockerfile and assign free host ports.
	containerPorts := make([]PortSpec, 0, len(requestedPorts))
	for _, p := range requestedPorts {
		sp := p
		if sp.Protocol == "" {
			sp.Protocol = "tcp"
		}
		containerPorts = append(containerPorts, sp)
	}
	if len(containerPorts) == 0 {
		for _, cp := range buildPorts {
			containerPorts = append(containerPorts, PortSpec{ContainerPort: cp, Protocol: "tcp"})
		}
	}
	for i := range containerPorts {
		if containerPorts[i].HostPort == 0 {
			containerPorts[i].HostPort = freePort()
		}
	}

	// 2. Transition to STARTING before container creation begins (DL-02, G-19)
	dep.Status = StatusStarting
	dep.Stage = "STARTING"
	if err := s.depRepo.UpdateStatus(ctx, depID, StatusStarting, "STARTING"); err != nil {
		s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to update deployment status to STARTING")
	}

	if s.crashHook != nil {
		if err := s.crashHook(StatusStarting); err != nil {
			return nil, err
		}
	}

	instanceCount := dep.InstanceCount
	if instanceCount <= 0 {
		instanceCount = 1
	}

	// 3. Schedule and run instances
	for i := 0; i < instanceCount; i++ {
		instID := uuid.New().String()
		instKey := fmt.Sprintf("inst-%s-%d", depID[:8], i)

		inst := &Instance{
			ID:           instID,
			DeploymentID: depID,
			InstanceKey:  instKey,
			Status:       "PENDING",
		}

		if err := s.instRepo.Create(ctx, inst); err != nil {
			if uErr := s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "FAILED"); uErr != nil {
				s.log.Error().Err(uErr).Str("deployment_id", depID).Msg("failed to update deployment status to FAILED")
			}
			dep.Status = StatusFailed
			dep.Stage = "FAILED"
			return createdInstances, fmt.Errorf("failed to create instance record: %w", err)
		}

		// Select worker via Scheduler
		schedCtx, schedSpan := observability.StartSpan(ctx, "scheduler.place")
		targetWorker, err := s.sched.SelectWorker(schedCtx, scheduler.WorkloadRequirement{
			DeploymentID:     depID,
			RequiredCapacity: 1,
			RequiredLabels:   dep.Labels,
		})
		schedSpan.End()
		if err != nil {
			inst.Status = "FAILED"
			if uErr := s.instRepo.Update(ctx, inst); uErr != nil {
				s.log.Error().Err(uErr).Str("instance_id", inst.ID).Msg("failed to update instance status to FAILED")
			}
			if uErr := s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "SCHEDULING_FAILED"); uErr != nil {
				s.log.Error().Err(uErr).Str("deployment_id", depID).Msg("failed to update deployment status to SCHEDULING_FAILED")
			}
			observability.MetricDeploymentStatusTotal.Inc(map[string]string{"status": string(StatusFailed), "project_id": dep.ProjectID})
			dep.Status = StatusFailed
			dep.Stage = "SCHEDULING_FAILED"
			return createdInstances, fmt.Errorf("scheduler failed for instance %s: %w", instKey, err)
		}

		if targetWorker != nil {
			observability.MetricSchedulerPlacementTotal.Inc(map[string]string{
				"worker_id": targetWorker.ID,
				"strategy":  "SPREADING",
			})
		}

		// Dispatch RunContainer to worker
		client, err := s.clientFactory.GetClient(ctx, targetWorker)
		if err != nil {
			inst.Status = "FAILED"
			if uErr := s.instRepo.Update(ctx, inst); uErr != nil {
				s.log.Error().Err(uErr).Str("instance_id", inst.ID).Msg("failed to update instance status to FAILED")
			}
			if uErr := s.depRepo.UpdateStatus(ctx, depID, StatusFailed, "FAILED"); uErr != nil {
				s.log.Error().Err(uErr).Str("deployment_id", depID).Msg("failed to update deployment status to FAILED")
			}
			observability.MetricDeploymentStatusTotal.Inc(map[string]string{"status": string(StatusFailed), "project_id": dep.ProjectID})
			dep.Status = StatusFailed
			dep.Stage = "FAILED"
			return createdInstances, fmt.Errorf("failed to connect to worker %s: %w", targetWorker.WorkerKey, err)
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
		runtimeEnv := dep.Env
		if s.secretStore != nil {
			if injected, err := secrets.InjectSecrets(ctx, s.secretStore, depID, dep.Env); err == nil {
				runtimeEnv = injected
			}
		}

		runLabels := make(map[string]string)
		for k, v := range dep.Labels {
			runLabels[k] = v
		}
		if digest != "" {
			runLabels["nebula.image_digest"] = digest
		}
		if signature != "" {
			runLabels["nebula.signature"] = signature
		}

		workerCtx, workerSpan := observability.StartSpan(ctx, "worker.run_container")
		runResp, err := client.RunContainer(workerCtx, &proto.RunContainerRequest{
			InstanceId:   instKey,
			DeploymentId: depID,
			Image:        dep.Image,
			Env:          runtimeEnv,
			Labels:       runLabels,
			Ports:        ports,
		})
		workerSpan.End()

		if err != nil || (runResp != nil && runResp.Error != "") {
			inst.Status = "FAILED"
			if uErr := s.instRepo.Update(ctx, inst); uErr != nil {
				s.log.Error().Err(uErr).Str("instance_id", inst.ID).Msg("failed to update instance status to FAILED")
			}
			failStage := "FAILED"
			errMsg := ""
			if err != nil {
				errMsg = err.Error()
			} else if runResp != nil {
				errMsg = runResp.Error
			}
			if strings.Contains(strings.ToLower(errMsg), "registry") && strings.Contains(strings.ToLower(errMsg), "unavailable") {
				failStage = "REGISTRY_UNAVAILABLE"
			}
			if strings.Contains(strings.ToLower(errMsg), "pull") || strings.Contains(strings.ToLower(errMsg), "digest") || strings.Contains(strings.ToLower(errMsg), "image") {
				observability.MetricImagePullFailureTotal.Inc(map[string]string{
					"image_ref": dep.Image,
					"reason":    "pull_failed",
				})
			}
			observability.MetricDeploymentStatusTotal.Inc(map[string]string{"status": string(StatusFailed), "project_id": dep.ProjectID})
			if uErr := s.depRepo.UpdateStatus(ctx, depID, StatusFailed, failStage); uErr != nil {
				s.log.Error().Err(uErr).Str("deployment_id", depID).Msg("failed to update deployment status to " + failStage)
			}
			dep.Status = StatusFailed
			dep.Stage = failStage
			if err != nil {
				return createdInstances, fmt.Errorf("worker RunContainer RPC failed: %w", err)
			}
			return createdInstances, fmt.Errorf("worker RunContainer error: %s", runResp.Error)
		}

		// Update instance with assigned worker and container ID
		inst.WorkerID = targetWorker.ID
		inst.ContainerID = runResp.ContainerId
		inst.Status = "RUNNING"
		if err := s.instRepo.Update(ctx, inst); err != nil {
			s.log.Error().Err(err).Str("instance_id", inst.ID).Msg("failed to update instance record")
		}

		// Register target in load balancer (OW-01)
		if s.router != nil && dep.ProjectID != "" {
			host := targetWorker.IPAddress
			if host == "" {
				host = "localhost"
			}
			appPort := 8080
			if len(containerPorts) > 0 && containerPorts[0].HostPort > 0 {
				appPort = containerPorts[0].HostPort
			}
			targetURL := fmt.Sprintf("http://%s:%d", host, appPort)
			if err := s.router.RegisterTarget(dep.ProjectID, inst.ID, targetURL); err != nil {
				s.log.Error().Err(err).Str("project_id", dep.ProjectID).Str("instance_id", inst.ID).Msg("failed to register target in router")
			}
		}

		// Increment active workload on worker
		s.registry.UpdateWorkload(targetWorker.WorkerKey, 1)

		createdInstances = append(createdInstances, inst)
	}

	// Update deployment status to RUNNING (DL-01, G-05)
	dep.Status = StatusRunning
	dep.Stage = "RUNNING"
	if err := s.depRepo.UpdateStatus(ctx, depID, StatusRunning, "RUNNING"); err != nil {
		s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to update deployment status to RUNNING")
	}

	if s.eventRepo != nil {
		_ = s.eventRepo.Create(ctx, &Event{
			ProjectID:    dep.ProjectID,
			DeploymentID: depID,
			EventType:    "DEPLOYMENT_RUNNING",
			Message:      fmt.Sprintf("Deployment %s converged to RUNNING with %d instances", depID[:8], len(createdInstances)),
		})
	}

	observability.MetricDeploymentStatusTotal.Inc(map[string]string{
		"status":     string(StatusRunning),
		"project_id": dep.ProjectID,
	})

	if s.crashHook != nil {
		if err := s.crashHook(StatusRunning); err != nil {
			return createdInstances, err
		}
	}

	return createdInstances, nil
}

// StopDeployment stops an active deployment and all its running instances (§8.2).
func (s *Service) StopDeployment(ctx context.Context, depID string) (*Deployment, error) {
	dep, err := s.depRepo.GetByID(ctx, depID)
	if err != nil {
		return nil, err
	}

	lock := s.getProjectLock(dep.ProjectID)
	lock.Lock()
	defer lock.Unlock()

	// Stop all associated instances on workers
	instances, err := s.instRepo.ListByDeployment(ctx, depID)
	if err == nil {
		for _, inst := range instances {
			if inst.WorkerID != "" {
				if w, ok := s.registry.Get(inst.WorkerID); ok {
					if client, cErr := s.clientFactory.GetClient(ctx, w); cErr == nil {
						_, _ = client.StopContainer(ctx, &proto.StopContainerRequest{
							InstanceId:     inst.InstanceKey,
							TimeoutSeconds: 5,
						})
					}
				}
			}
			inst.Status = "STOPPED"
			_ = s.instRepo.Update(ctx, inst)
			if s.router != nil && dep.ProjectID != "" {
				s.router.UnregisterTarget(dep.ProjectID, inst.ID)
			}
		}
	}

	dep.Status = StatusStopped
	dep.Stage = "STOPPED"
	dep.DesiredState = "STOPPED"
	if err := s.depRepo.UpdateStatus(ctx, depID, StatusStopped, "STOPPED"); err != nil {
		s.log.Error().Err(err).Str("deployment_id", depID).Msg("failed to update deployment status to STOPPED")
	}

	s.log.Info().Str("deployment_id", depID).Msg("deployment stopped successfully (§8.2)")
	return dep, nil
}

// Rollback executes an operational rollback (§27.2):
// Selects previous known-good release by recorded digest, reschedules and starts new instances
// reusing the existing scheduler/worker path, routes traffic once healthy, stops old containers,
// updates old deployment to ROLLED_BACK, and records a rollback event.
func (s *Service) Rollback(ctx context.Context, depID string) (*Deployment, []*Instance, error) {
	dep, err := s.depRepo.GetByID(ctx, depID)
	if err != nil {
		return nil, nil, err
	}

	lock := s.getProjectLock(dep.ProjectID)
	lock.Lock()
	defer lock.Unlock()

	if dep.Status == StatusRolledBack {
		return nil, nil, fmt.Errorf("deployment %s has already been rolled back", depID)
	}

	// 1. Select previous known-good release (§27.2)
	releases, err := s.releaseRepo.ListByProject(ctx, dep.ProjectID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list releases for project %s: %w", dep.ProjectID, err)
	}

	var prevRelease *Release
	for _, rel := range releases {
		// Skip current deployment or releases without valid recorded digest
		if rel.DeploymentID == dep.ID || rel.ImageDigest == "" {
			continue
		}
		// Confirm release was known-good: skip past any releases whose deployment failed (§27.2)
		if rel.DeploymentID != "" {
			candDep, cErr := s.depRepo.GetByID(ctx, rel.DeploymentID)
			if cErr == nil && candDep != nil && candDep.Status == StatusFailed {
				s.log.Debug().
					Str("skipped_failed_deployment_id", rel.DeploymentID).
					Str("version", rel.Version).
					Msg("skipping failed release candidate in rollback search (§27.2)")
				continue
			}
		}
		prevRelease = rel
		break
	}

	if prevRelease == nil {
		return nil, nil, fmt.Errorf("no previous known-good release found for project %s", dep.ProjectID)
	}

	// 2. Create new deployment record representing the restored release (§26.3, §27.2)
	rollbackDepID := uuid.New().String()
	instanceCount := dep.InstanceCount
	if instanceCount <= 0 {
		instanceCount = 1
	}

	newDep := &Deployment{
		ID:            rollbackDepID,
		ProjectID:     dep.ProjectID,
		Revision:      prevRelease.Version,
		Image:         prevRelease.ImageRef,
		ImageDigest:   prevRelease.ImageDigest,
		DesiredState:  "RUNNING",
		Status:        StatusQueued,
		Stage:         "QUEUED",
		InstanceCount: instanceCount,
		Env:           dep.Env,
		Labels:        dep.Labels,
	}

	if err := s.depRepo.Create(ctx, newDep); err != nil {
		return nil, nil, fmt.Errorf("failed to persist rollback deployment: %w", err)
	}

	// Snapshot project secrets for rollback deployment
	if s.secretStore != nil && dep.ProjectID != "" {
		_ = s.secretStore.SnapshotForDeployment(ctx, dep.ProjectID, rollbackDepID)
	}

	// 3. Schedule and run instances from recorded digest (reuses existing scheduler/worker path)
	newInstances, err := s.scheduleAndDispatchInstances(ctx, newDep, nil, nil, prevRelease.ImageDigest, prevRelease.Signature)
	if err != nil {
		return newDep, nil, fmt.Errorf("failed to schedule and run rollback instances: %w", err)
	}

	// 4. Stop old instances of the superseded/failed deployment and remove old routing targets
	oldInstances, _ := s.instRepo.ListByDeployment(ctx, dep.ID)
	for _, oldInst := range oldInstances {
		if oldInst.WorkerID != "" {
			if w, ok := s.registry.Get(oldInst.WorkerID); ok {
				if client, cErr := s.clientFactory.GetClient(ctx, w); cErr == nil {
					_, _ = client.StopContainer(ctx, &proto.StopContainerRequest{
						InstanceId:     oldInst.InstanceKey,
						TimeoutSeconds: 5,
					})
				}
			}
		}
		oldInst.Status = "STOPPED"
		_ = s.instRepo.Update(ctx, oldInst)
		if s.router != nil && dep.ProjectID != "" {
			s.router.UnregisterTarget(dep.ProjectID, oldInst.ID)
		}
	}

	// 5. Update old deployment to ROLLED_BACK
	dep.Status = StatusRolledBack
	dep.Stage = "ROLLED_BACK"
	dep.DesiredState = "STOPPED"
	if err := s.depRepo.UpdateStatus(ctx, dep.ID, StatusRolledBack, "ROLLED_BACK"); err != nil {
		s.log.Error().Err(err).Str("deployment_id", dep.ID).Msg("failed to update status to ROLLED_BACK")
	}

	// 6. Record rollback event (§27.2)
	if s.eventRepo != nil {
		ev := &Event{
			ID:           uuid.New().String(),
			ProjectID:    dep.ProjectID,
			DeploymentID: dep.ID,
			EventType:    "DEPLOYMENT_ROLLED_BACK",
			Message:      fmt.Sprintf("Deployment %s rolled back to release %s (digest %s)", dep.ID, prevRelease.Version, prevRelease.ImageDigest),
			Metadata: map[string]interface{}{
				"source_deployment_id":   dep.ID,
				"restored_deployment_id": newDep.ID,
				"target_version":         prevRelease.Version,
				"target_image":           prevRelease.ImageRef,
				"target_digest":          prevRelease.ImageDigest,
				"previous_release_id":    prevRelease.ID,
			},
			CreatedAt: time.Now().UTC(),
		}
		if err := s.eventRepo.Create(ctx, ev); err != nil {
			s.log.Error().Err(err).Msg("failed to record rollback event")
		}
	}

	s.log.Info().
		Str("rolled_back_deployment_id", dep.ID).
		Str("new_deployment_id", newDep.ID).
		Str("restored_digest", prevRelease.ImageDigest).
		Int("instances", len(newInstances)).
		Msg("rollback completed successfully: traffic restored to previous release (§27.2)")

	return newDep, newInstances, nil
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

// ErrNoRunningDeployment is returned when attempting to scale a project with no running deployment.
var ErrNoRunningDeployment = fmt.Errorf("no running deployment found for project")

// ScaleDeployment scales a running deployment to the target replica count (§18).
// Scale-up schedules new instances via the existing scheduler pipeline and registers them with
// the load balancer only after confirming they are RUNNING (readiness check).
// Scale-down stops excess containers, unregisters them from the load balancer, and marks them STOPPED.
func (s *Service) ScaleDeployment(ctx context.Context, depID string, targetReplicas int) (*Deployment, []*Instance, error) {
	if targetReplicas <= 0 {
		return nil, nil, fmt.Errorf("target replica count must be greater than 0, got %d", targetReplicas)
	}

	dep, err := s.depRepo.GetByID(ctx, depID)
	if err != nil {
		return nil, nil, err
	}

	if dep.Status != StatusRunning {
		return nil, nil, fmt.Errorf("cannot scale deployment %s: status is %s (must be RUNNING)", depID, dep.Status)
	}

	lock := s.getProjectLock(dep.ProjectID)
	lock.Lock()
	defer lock.Unlock()

	// Re-fetch under lock to prevent lost updates (concurrency safeguard §9)
	dep, err = s.depRepo.GetByID(ctx, depID)
	if err != nil {
		return nil, nil, err
	}

	allInstances, err := s.instRepo.ListByDeployment(ctx, depID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list instances: %w", err)
	}

	var active []*Instance
	for _, inst := range allInstances {
		if inst.Status != "STOPPED" && inst.Status != "FAILED" {
			active = append(active, inst)
		}
	}

	currentCount := len(active)
	if currentCount == targetReplicas {
		dep.InstanceCount = targetReplicas
		dep.DesiredReplicas = targetReplicas
		_ = s.depRepo.UpdateScale(ctx, dep.ID, targetReplicas)
		return dep, active, nil
	}

	if targetReplicas > currentCount {
		// -------------------------------------------------------------
		// Scale Up: add (targetReplicas - currentCount) replicas
		// -------------------------------------------------------------
		delta := targetReplicas - currentCount
		for i := 0; i < delta; i++ {
			instID := uuid.New().String()
			instKey := fmt.Sprintf("inst-%s-%d-%d", dep.ID[:8], currentCount+i, time.Now().UnixNano()%10000)

			inst := &Instance{
				ID:           instID,
				DeploymentID: dep.ID,
				InstanceKey:  instKey,
				Status:       "PENDING",
			}
			if err := s.instRepo.Create(ctx, inst); err != nil {
				return dep, active, fmt.Errorf("failed to create instance record: %w", err)
			}

			// Schedule placement via existing scheduler (spreading policy G-08, G-31)
			targetWorker, err := s.sched.SelectWorker(ctx, scheduler.WorkloadRequirement{
				DeploymentID:     dep.ID,
				RequiredCapacity: 1,
				RequiredLabels:   dep.Labels,
			})
			if err != nil {
				inst.Status = "FAILED"
				_ = s.instRepo.Update(ctx, inst)
				return dep, active, fmt.Errorf("scheduler failed to select worker: %w", err)
			}

			client, err := s.clientFactory.GetClient(ctx, targetWorker)
			if err != nil {
				inst.Status = "FAILED"
				_ = s.instRepo.Update(ctx, inst)
				return dep, active, fmt.Errorf("failed to connect to worker %s: %w", targetWorker.WorkerKey, err)
			}

			hostPort := freePort()
			ports := []*proto.PortMapping{
				{ContainerPort: 8080, HostPort: int32(hostPort), Protocol: "tcp"},
			}

			runLabels := make(map[string]string)
			for k, v := range dep.Labels {
				runLabels[k] = v
			}
			if dep.ImageDigest != "" {
				runLabels["nebula.image_digest"] = dep.ImageDigest
			}

			runtimeEnv := dep.Env
			if s.secretStore != nil {
				if injected, sErr := secrets.InjectSecrets(ctx, s.secretStore, dep.ID, dep.Env); sErr == nil {
					runtimeEnv = injected
				}
			}

			runResp, err := client.RunContainer(ctx, &proto.RunContainerRequest{
				InstanceId:   instKey,
				DeploymentId: dep.ID,
				Image:        dep.Image,
				Env:          runtimeEnv,
				Labels:       runLabels,
				Ports:        ports,
			})
			if err != nil || (runResp != nil && runResp.Error != "") {
				inst.Status = "FAILED"
				_ = s.instRepo.Update(ctx, inst)
				errMsg := "run failed"
				if err != nil {
					errMsg = err.Error()
				} else if runResp != nil {
					errMsg = runResp.Error
				}
				return dep, active, fmt.Errorf("worker RunContainer failed: %s", errMsg)
			}

			// Readiness check (§18): confirmed RUNNING before adding to load balancer rotation
			inst.WorkerID = targetWorker.ID
			inst.ContainerID = runResp.ContainerId
			inst.Status = "RUNNING"
			if err := s.instRepo.Update(ctx, inst); err != nil {
				s.log.Error().Err(err).Msg("failed to update instance record")
			}

			if s.router != nil && dep.ProjectID != "" {
				host := targetWorker.IPAddress
				if host == "" {
					host = "127.0.0.1"
				}
				targetURL := fmt.Sprintf("http://%s:%d", host, hostPort)
				_ = s.router.RegisterTarget(dep.ProjectID, inst.ID, targetURL)
			}

			s.registry.UpdateWorkload(targetWorker.WorkerKey, 1)
			active = append(active, inst)
		}
	} else {
		// -------------------------------------------------------------
		// Scale Down: terminate (currentCount - targetReplicas) replicas
		// -------------------------------------------------------------
		delta := currentCount - targetReplicas
		toRemove := active[len(active)-delta:]
		remaining := active[:len(active)-delta]

		for _, inst := range toRemove {
			if s.router != nil && dep.ProjectID != "" {
				s.router.UnregisterTarget(dep.ProjectID, inst.ID)
			}

			if inst.WorkerID != "" {
				if w, ok := s.registry.Get(inst.WorkerID); ok {
					if client, cErr := s.clientFactory.GetClient(ctx, w); cErr == nil {
						_, _ = client.StopContainer(ctx, &proto.StopContainerRequest{
							InstanceId:     inst.InstanceKey,
							TimeoutSeconds: 5,
						})
					}
					s.registry.UpdateWorkload(w.WorkerKey, -1)
				}
			}

			inst.Status = "STOPPED"
			_ = s.instRepo.Update(ctx, inst)
		}
		active = remaining
	}

	dep.InstanceCount = targetReplicas
	dep.DesiredReplicas = targetReplicas
	if err := s.depRepo.UpdateScale(ctx, dep.ID, targetReplicas); err != nil {
		s.log.Error().Err(err).Msg("failed to persist updated deployment scale")
	}

	// Record scaling event (§19.1, §22, Gate G-33)
	if s.eventRepo != nil {
		evType := "DEPLOYMENT_SCALED"
		if targetReplicas > currentCount {
			evType = "SCALE_UP"
		} else {
			evType = "SCALE_DOWN"
		}
		ev := &Event{
			ID:           uuid.New().String(),
			ProjectID:    dep.ProjectID,
			DeploymentID: dep.ID,
			EventType:    evType,
			Message:      fmt.Sprintf("Scaled deployment %s from %d to %d replicas", dep.ID, currentCount, targetReplicas),
			Metadata: map[string]interface{}{
				"previous_replicas": currentCount,
				"desired_replicas":  targetReplicas,
				"timestamp":         time.Now().UTC(),
			},
			CreatedAt: time.Now().UTC(),
		}
		_ = s.eventRepo.Create(ctx, ev)
	}

	s.log.Info().
		Str("deployment_id", dep.ID).
		Str("project_id", dep.ProjectID).
		Int("previous", currentCount).
		Int("desired", targetReplicas).
		Msg("deployment scale adjusted successfully (§18)")

	return dep, active, nil
}

// ScaleProject scales the active running deployment of a project to the target replica count (§18).
func (s *Service) ScaleProject(ctx context.Context, projectID string, targetReplicas int) (*Deployment, []*Instance, error) {
	deps, err := s.depRepo.List(ctx, projectID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list deployments for project %s: %w", projectID, err)
	}

	var activeDep *Deployment
	for _, d := range deps {
		if d.Status == StatusRunning {
			activeDep = d
			break
		}
	}

	if activeDep == nil {
		return nil, nil, ErrNoRunningDeployment
	}

	return s.ScaleDeployment(ctx, activeDep.ID, targetReplicas)
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
