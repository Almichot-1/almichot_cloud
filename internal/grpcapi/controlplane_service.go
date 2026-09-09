package grpcapi

import (
	"context"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ControlPlaneServiceServer implements proto.ControlPlaneServiceServer.
type ControlPlaneServiceServer struct {
	proto.UnimplementedControlPlaneServiceServer
	registry   *workers.Registry
	depService *deployments.Service
	log        zerolog.Logger
}

// NewControlPlaneServiceServer creates a new ControlPlaneServiceServer.
func NewControlPlaneServiceServer(
	registry *workers.Registry,
	depService *deployments.Service,
	log zerolog.Logger,
) *ControlPlaneServiceServer {
	return &ControlPlaneServiceServer{
		registry:   registry,
		depService: depService,
		log:        log.With().Str("service", "ControlPlaneService").Logger(),
	}
}

// RegisterWorker registers a worker into the worker registry.
func (s *ControlPlaneServiceServer) RegisterWorker(ctx context.Context, req *proto.RegisterWorkerRequest) (*proto.RegisterWorkerResponse, error) {
	if req.WorkerKey == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_key is required")
	}

	w, err := s.registry.Register(ctx, workers.RegisterParams{
		WorkerKey: req.WorkerKey,
		Hostname:  req.Hostname,
		IPAddress: req.IpAddress,
		GRPCPort:  int(req.GrpcPort),
		Capacity:  int(req.Capacity),
		Labels:    req.Labels,
	})
	if err != nil {
		s.log.Error().Err(err).Str("worker_key", req.WorkerKey).Msg("failed to register worker")
		return nil, status.Errorf(codes.Internal, "failed to register worker: %v", err)
	}

	return &proto.RegisterWorkerResponse{
		WorkerId: w.ID,
		Success:  true,
		Message:  "worker registered successfully",
	}, nil
}

// Heartbeat accepts and stores worker heartbeat timestamps.
func (s *ControlPlaneServiceServer) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	if err := s.registry.Heartbeat(ctx, req.WorkerId); err != nil {
		return &proto.HeartbeatResponse{Success: false}, nil
	}

	return &proto.HeartbeatResponse{
		Success:    true,
		RecordedAt: time.Now().Unix(),
	}, nil
}

// DrainWorker sets a worker to DRAINING state and marks it unschedulable (G-07).
func (s *ControlPlaneServiceServer) DrainWorker(ctx context.Context, req *proto.DrainWorkerRequest) (*proto.DrainWorkerResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	w, err := s.registry.Drain(ctx, req.WorkerId)
	if err != nil {
		s.log.Error().Err(err).Str("worker_id", req.WorkerId).Msg("failed to drain worker")
		return nil, status.Errorf(codes.NotFound, "failed to drain worker: %v", err)
	}

	return &proto.DrainWorkerResponse{
		WorkerId: w.ID,
		Success:  true,
		State:    string(w.State),
	}, nil
}

// CreateDeployment accepts a deployment request, schedules instances, and dispatches containers.
func (s *ControlPlaneServiceServer) CreateDeployment(ctx context.Context, req *proto.CreateDeploymentRequest) (*proto.CreateDeploymentResponse, error) {
	if req.Image == "" && req.SourcePath == "" {
		return nil, status.Error(codes.InvalidArgument, "either image or source_path is required")
	}

	dep, instances, err := s.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
		ProjectID:     req.ProjectId,
		SourcePath:    req.SourcePath,
		Image:         req.Image,
		InstanceCount: int(req.InstanceCount),
		Env:           req.Env,
		Labels:        req.Labels,
	})
	if err != nil {
		s.log.Error().Err(err).Str("image", req.Image).Msg("deployment creation failed")
		depID := ""
		if dep != nil {
			depID = dep.ID
		}
		return &proto.CreateDeploymentResponse{
			DeploymentId: depID,
			Status:       "FAILED",
			Error:        err.Error(),
		}, nil
	}

	instanceIDs := make([]string, 0, len(instances))
	for _, inst := range instances {
		instanceIDs = append(instanceIDs, inst.ID)
	}

	return &proto.CreateDeploymentResponse{
		DeploymentId: dep.ID,
		Status:       string(dep.Status),
		InstanceIds:  instanceIDs,
		ImageDigest:  dep.ImageDigest,
	}, nil
}

// GetDeployment retrieves a deployment's status and its running instances.
func (s *ControlPlaneServiceServer) GetDeployment(ctx context.Context, req *proto.GetDeploymentRequest) (*proto.GetDeploymentResponse, error) {
	if req.DeploymentId == "" {
		return nil, status.Error(codes.InvalidArgument, "deployment_id is required")
	}

	dep, instances, err := s.depService.GetDeployment(ctx, req.DeploymentId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "deployment not found: %v", err)
	}

	instanceInfos := make([]*proto.DeploymentInstanceInfo, 0, len(instances))
	for _, inst := range instances {
		instanceInfos = append(instanceInfos, &proto.DeploymentInstanceInfo{
			InstanceId:  inst.ID,
			WorkerId:    inst.WorkerID,
			Status:      inst.Status,
			ContainerId: inst.ContainerID,
		})
	}

	return &proto.GetDeploymentResponse{
		DeploymentId: dep.ID,
		ProjectId:    dep.ProjectID,
		Image:        dep.Image,
		Status:       string(dep.Status),
		Instances:    instanceInfos,
	}, nil
}
