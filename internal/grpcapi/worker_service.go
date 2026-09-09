package grpcapi

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WorkerServiceServer implements the proto.WorkerServiceServer interface.
type WorkerServiceServer struct {
	proto.UnimplementedWorkerServiceServer
	ops       *runtime.ContainerOps
	workerKey string
	log       zerolog.Logger
}

// NewWorkerServiceServer creates a new WorkerServiceServer.
func NewWorkerServiceServer(ops *runtime.ContainerOps, log zerolog.Logger) *WorkerServiceServer {
	return &WorkerServiceServer{
		ops: ops,
		log: log.With().Str("service", "WorkerService").Logger(),
	}
}

// SetWorkerKey sets the unique worker identity for multi-worker node isolation.
func (s *WorkerServiceServer) SetWorkerKey(key string) {
	s.workerKey = key
}

var _ proto.WorkerServiceServer = (*WorkerServiceServer)(nil)

// RunContainer starts a container for an instance or returns the existing container if already running (G-09).
func (s *WorkerServiceServer) RunContainer(ctx context.Context, req *proto.RunContainerRequest) (*proto.RunContainerResponse, error) {
	if req.InstanceId == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_id is required")
	}
	if req.Image == "" {
		return nil, status.Error(codes.InvalidArgument, "image is required")
	}

	envVars := make([]string, 0, len(req.Env))
	for k, v := range req.Env {
		envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
	}

	ports := make([]runtime.PortMapping, 0, len(req.Ports))
	for _, p := range req.Ports {
		ports = append(ports, runtime.PortMapping{
			HostPort:      int(p.HostPort),
			ContainerPort: int(p.ContainerPort),
			Protocol:      p.Protocol,
		})
	}

	labels := make(map[string]string)
	for k, v := range req.Labels {
		labels[k] = v
	}
	if s.workerKey != "" {
		labels["nebula.worker_key"] = s.workerKey
	}

	var cmd []string
	if cmdStr, ok := req.Labels["nebula.cmd"]; ok && cmdStr != "" {
		cmd = strings.Fields(cmdStr)
	}

	runOpts := runtime.RunOptions{
		InstanceID:   req.InstanceId,
		DeploymentID: req.DeploymentId,
		Image:        req.Image,
		Cmd:          cmd,
		Env:          envVars,
		Ports:        ports,
		Labels:       labels,
	}

	result, err := s.ops.RunContainer(ctx, runOpts)
	if err != nil {
		s.log.Error().
			Err(err).
			Str("instance_id", req.InstanceId).
			Msg("failed to run container")
		containerID := ""
		if result != nil {
			containerID = result.ContainerID
		}
		return &proto.RunContainerResponse{
			InstanceId:  req.InstanceId,
			ContainerId: containerID,
			Status:      "FAILED",
			Error:       err.Error(),
		}, nil
	}

	return &proto.RunContainerResponse{
		InstanceId:  result.InstanceID,
		ContainerId: result.ContainerID,
		Status:      result.Status,
		IsDuplicate: result.IsDuplicate,
	}, nil
}

// StopContainer stops an instance's container.
func (s *WorkerServiceServer) StopContainer(ctx context.Context, req *proto.StopContainerRequest) (*proto.StopContainerResponse, error) {
	if req.InstanceId == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_id is required")
	}

	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	res, err := s.ops.StopContainer(ctx, req.InstanceId, timeout)
	if err != nil {
		s.log.Error().
			Err(err).
			Str("instance_id", req.InstanceId).
			Msg("failed to stop container")
		return &proto.StopContainerResponse{
			InstanceId: req.InstanceId,
			Success:    false,
			Error:      err.Error(),
		}, nil
	}

	return &proto.StopContainerResponse{
		InstanceId: res.InstanceID,
		Success:    res.Success,
	}, nil
}

// GetContainerStatus retrieves the current container status for an instance.
func (s *WorkerServiceServer) GetContainerStatus(ctx context.Context, req *proto.GetContainerStatusRequest) (*proto.GetContainerStatusResponse, error) {
	if req.InstanceId == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_id is required")
	}

	res, err := s.ops.GetContainerStatus(ctx, req.InstanceId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get container status: %v", err)
	}

	return &proto.GetContainerStatusResponse{
		InstanceId:  res.InstanceID,
		ContainerId: res.ContainerID,
		Status:      res.Status,
		ExitCode:    int32(res.ExitCode),
		Error:       res.Error,
	}, nil
}

// ListContainers lists all containers tracked on this worker.
func (s *WorkerServiceServer) ListContainers(ctx context.Context, req *proto.ListContainersRequest) (*proto.ListContainersResponse, error) {
	if client := s.ops.Client(); client != nil {
		summaries, err := client.ListContainers(ctx)
		if err == nil {
			containers := make([]*proto.ContainerInfo, 0, len(summaries))
			for _, sum := range summaries {
				if s.workerKey != "" && sum.Labels != nil {
					if owner, ok := sum.Labels["nebula.worker_key"]; ok && owner != s.workerKey {
						continue // skip containers belonging to a different worker
					}
				}
				instanceID := sum.InstanceID
				if instanceID == "" && sum.Labels != nil {
					instanceID = sum.Labels["nebula.instance_id"]
				}
				containers = append(containers, &proto.ContainerInfo{
					InstanceId:  instanceID,
					ContainerId: sum.ID,
					Image:       sum.Image,
					Status:      sum.State,
					Labels:      sum.Labels,
					CreatedAt:   sum.CreatedAt.Unix(),
				})
			}
			return &proto.ListContainersResponse{
				Containers: containers,
			}, nil
		}
	}

	records, err := s.ops.ListContainers(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list containers: %v", err)
	}

	containers := make([]*proto.ContainerInfo, 0, len(records))
	for _, rec := range records {
		containers = append(containers, &proto.ContainerInfo{
			InstanceId:  rec.InstanceID,
			ContainerId: rec.ContainerID,
			Image:       rec.Image,
			Status:      string(rec.State),
			Labels:      rec.Labels,
			CreatedAt:   rec.CreatedAt.Unix(),
		})
	}

	return &proto.ListContainersResponse{
		Containers: containers,
	}, nil
}


// Register acknowledges worker registration (no CP-side registry yet, just accept the call).
func (s *WorkerServiceServer) Register(ctx context.Context, req *proto.RegisterRequest) (*proto.RegisterResponse, error) {
	s.log.Info().
		Str("worker_id", req.WorkerId).
		Str("hostname", req.Hostname).
		Str("ip_address", req.IpAddress).
		Int32("capacity", req.Capacity).
		Msg("worker registration accepted")

	return &proto.RegisterResponse{
		Success: true,
		Message: fmt.Sprintf("worker %s registered successfully", req.WorkerId),
	}, nil
}
