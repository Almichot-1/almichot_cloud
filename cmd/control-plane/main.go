package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nebula/nebula/internal/api"
	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/build"
	"github.com/nebula/nebula/internal/config"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/grpcapi"
	"github.com/nebula/nebula/internal/observability"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/secrets"
	"github.com/nebula/nebula/internal/storage"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.Load()
	log := observability.New(cfg.App.Env).With().Str("component", "control-plane").Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info().Str("env", cfg.App.Env).Msg("control-plane starting")

	var workerRepo workers.WorkerRepository = workers.NewMemoryWorkerRepository()
	var depRepo deployments.DeploymentRepository = deployments.NewMemoryDeploymentRepository()
	var instRepo deployments.InstanceRepository = deployments.NewMemoryInstanceRepository()
	var projectRepo projects.ProjectRepository = projects.NewMemoryProjectRepository()

	keyProvider := secrets.NewSingleKeyProvider([]byte(cfg.Secrets.MasterKey))
	var secretStore secrets.SecretStore = secrets.NewMemorySecretStore(keyProvider)
	redactor := secrets.NewRedactor()

	if os.Getenv("NEBULA_DB_DISABLED") != "true" {
		store, err := storage.Open(ctx, cfg.Postgres, log)
		if err != nil {
			if cfg.App.Env == "development" {
				log.Warn().Err(err).Msg("postgres connection skipped in development mode; using in-memory repositories")
			} else {
				log.Fatal().Err(err).Msg("failed to connect to postgres")
			}
		} else {
			defer store.Close()
			if err := storage.Migrate(ctx, store.Pool); err != nil {
				log.Fatal().Err(err).Msg("failed to apply migrations")
			}
			log.Info().Msg("migrations applied successfully")

			workerRepo = storage.NewPostgresWorkerRepository(store.Pool)
			depRepo = storage.NewPostgresDeploymentRepository(store.Pool)
			instRepo = storage.NewPostgresInstanceRepository(store.Pool)
			projectRepo = storage.NewPostgresProjectRepository(store.Pool)
			secretStore = storage.NewPostgresSecretStore(store.Pool, keyProvider)
		}
	}

	workerRegistry := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(workerRegistry, instRepo.CountByWorkerForDeployment, log)

	workerClientFactory := deployments.NewGRPCWorkerClientFactory()
	defer workerClientFactory.Close()

	depService := deployments.NewService(depRepo, instRepo, workerRegistry, sched, workerClientFactory, log)
	depService.SetSecretStore(secretStore)
	depService.SetRedactor(redactor)

	// Build sandbox: prefer a real Docker build, fall back to the simulated sandbox.
	if os.Getenv("NEBULA_BUILD_SIMULATED") == "true" {
		log.Info().Msg("NEBULA_BUILD_SIMULATED=true; using simulated build sandbox")
	} else if realSandbox, err := build.NewRealDockerSandbox(log); err == nil {
		defer realSandbox.Close()
		orchestrator := build.NewOrchestrator(realSandbox, log)
		depService.SetBuildAndRegistry(orchestrator, depService.RegistryClient(), depService.Router())
		log.Info().Msg("real docker build sandbox enabled")
	} else {
		log.Warn().Err(err).Msg("docker daemon unavailable; using simulated build sandbox")
	}

	// CP crash recovery sequence: detect and resolve any in-flight stuck deployments (DL-03, G-16..G-20)
	recoveryEngine := deployments.NewRecoveryEngine(depRepo, instRepo, workerRegistry, sched, workerClientFactory, log)
	if report, err := recoveryEngine.RecoverDeployments(ctx, deployments.RecoveryPolicyFailSafe); err != nil {
		log.Warn().Err(err).Msg("startup deployment crash recovery encountered error")
	} else if report.TotalStuckDetected > 0 {
		log.Info().
			Int("stuck_detected", report.TotalStuckDetected).
			Int("queued_recovered", report.QueuedRecovered).
			Int("building_recovered", report.BuildingRecovered).
			Int("scheduling_recovered", report.SchedulingRecovered).
			Int("starting_recovered", report.StartingRecovered).
			Int("orphans_stopped", report.OrphanContainersStopped).
			Msg("startup deployment crash recovery completed (DL-03)")
	}

	// CP startup sequence: full reconcile before scheduling resumes (G-04)
	reconciler := reconcile.NewReconciler(workerRegistry, depRepo, instRepo, sched, workerClientFactory, log)
	reconciler.SetInterval(cfg.Reconcile.Interval)
	reconciler.SetTimeout(cfg.Reconcile.Timeout)
	if actions, err := reconciler.ReconcileOnce(ctx); err != nil {
		log.Warn().Err(err).Msg("initial startup reconciliation pass failed")
	} else {
		log.Info().
			Int("recreated", actions.RecreatedCount).
			Int("stopped", actions.StoppedCount).
			Int("flagged", actions.FlaggedCount).
			Msg("initial startup reconciliation converged")
	}
	reconciler.Start(ctx)
	defer reconciler.Stop()

	cpService := grpcapi.NewControlPlaneServiceServer(workerRegistry, depService, log)

	server, err := grpcapi.NewServer(cfg.GRPC.ListenAddr, log, func(s *grpc.Server) {
		proto.RegisterControlPlaneServiceServer(s, cpService)
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create control-plane gRPC server")
	}

	go func() {
		if err := server.Serve(); err != nil {
			log.Error().Err(err).Msg("control-plane gRPC server stopped")
		}
	}()

	tokenStore := auth.NewTokenStore()
	if cfg.Auth.AdminToken != "" {
		tokenStore.RegisterToken(cfg.Auth.AdminToken, &auth.User{
			ID:              "admin",
			Username:        "admin",
			Role:            "admin",
			AllowedProjects: []string{"*"},
		})
	}
	var authenticator auth.Authenticator
	if cfg.Auth.Enabled {
		authenticator = auth.NewTokenAuthenticator(tokenStore)
	}

	apiServer := api.NewServer(depService, projectRepo, workerRegistry, depService.Router(), log)
	if authenticator != nil {
		apiServer.SetAuthenticator(authenticator)
	}
	apiServer.SetSecretStore(secretStore)
	apiServer.SetRedactor(redactor)

	httpServer := &http.Server{
		Addr:              cfg.HTTP.ListenAddr,
		Handler:           api.NewRouter(apiServer),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info().Str("http_addr", cfg.HTTP.ListenAddr).Msg("control-plane HTTP API started")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("control-plane HTTP API stopped")
		}
	}()

	log.Info().Str("grpc_addr", server.Addr()).Msg("control-plane started")

	<-ctx.Done()
	log.Info().Msg("control-plane shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	server.GracefulStop()
}
