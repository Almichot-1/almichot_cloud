package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/nebula/nebula/internal/config"
	"github.com/nebula/nebula/internal/grpcapi"
	"github.com/nebula/nebula/internal/observability"
	"github.com/nebula/nebula/internal/runtime"
	"github.com/nebula/nebula/proto"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.Load()
	log := observability.New(cfg.App.Env).With().Str("component", "worker-agent").Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info().Str("env", cfg.App.Env).Str("worker_id", cfg.Worker.ID).Msg("worker-agent starting")

	// Docker client: real by default, mock only when explicitly requested.
	var dockerCli runtime.DockerClient
	if os.Getenv("NEBULA_DOCKER_MOCK") == "true" {
		log.Info().Msg("using MockDockerClient as specified by NEBULA_DOCKER_MOCK")
		dockerCli = runtime.NewMockDockerClient()
	} else {
		realCli, err := runtime.NewRealDockerClient(log)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to connect to Docker daemon")
		}
		dockerCli = realCli
		defer realCli.Close()
	}

	tracker := runtime.NewInstanceTracker()
	ops := runtime.NewContainerOps(dockerCli, tracker, log)
	workerSvc := grpcapi.NewWorkerServiceServer(ops, log)

	server, err := grpcapi.NewServer(cfg.Worker.ListenAddr, log, func(s *grpc.Server) {
		proto.RegisterWorkerServiceServer(s, workerSvc)
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create gRPC server")
	}

	go func() {
		if err := server.Serve(); err != nil {
			log.Error().Err(err).Msg("gRPC server stopped")
		}
	}()

	log.Info().Str("grpc_addr", server.Addr()).Msg("worker-agent started")

	<-ctx.Done()
	log.Info().Msg("worker-agent shutting down")
	server.GracefulStop()
}
