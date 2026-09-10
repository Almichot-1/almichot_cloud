package integration

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/storage"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// CP-01..06 (G-04) + DA-01 (G-24) with REAL Postgres as the sole source of desired state:
// a CP restart (fresh registry + fresh repository handles against the same database) reloads
// deployments/instances/workers from Postgres, performs a full reconcile before scheduling
// resumes, and converges to zero actions on the immediate second pass.
func TestCPPostgres_G04_G24_RestartReconcileOverRealPostgres(t *testing.T) {
	adminURL := os.Getenv("NEBULA_TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("NEBULA_TEST_DATABASE_URL not set; skipping Postgres restore test")
	}

	log := zerolog.Nop()

	adminU, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("parse admin URL: %v", err)
	}
	adminConn, err := pgx.Connect(context.Background(), adminURL)
	if err != nil {
		t.Fatalf("connect to admin db: %v", err)
	}
	dbName := fmt.Sprintf("nebula_restart_%d", time.Now().UnixNano())
	if _, err := adminConn.Exec(context.Background(), "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create throwaway db: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminConn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
		_ = adminConn.Close(context.Background())
	})

	target := *adminU
	target.Path = "/" + dbName

	// -------------------------------------------------------------
	// Epoch 1: A Control Plane writes desired state into Postgres.
	// -------------------------------------------------------------
	pool1, err := pgxpool.New(context.Background(), target.String())
	if err != nil {
		t.Fatalf("pool1: %v", err)
	}
	defer pool1.Close()
	if err := storage.Migrate(context.Background(), pool1); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	projectRepo1 := storage.NewPostgresProjectRepository(pool1)
	depRepo1 := storage.NewPostgresDeploymentRepository(pool1)
	instRepo1 := storage.NewPostgresInstanceRepository(pool1)
	workerRepo1 := storage.NewPostgresWorkerRepository(pool1)

	mockFactory := deployments.NewMockWorkerClientFactory()

	reg1 := workers.NewRegistry(workerRepo1, log)
	w, err := reg1.Register(context.Background(), workers.RegisterParams{
		WorkerKey: "node-pg-restart-1",
		Hostname:  "pg-node",
		IPAddress: "10.20.0.1",
		Capacity:  10,
	})
	if err != nil {
		t.Fatalf("register worker (epoch 1): %v", err)
	}

	project := &projects.Project{Name: "phase4-pg-source", Description: "restart e2e", RepoURL: "https://example.com/pg"}
	if err := projectRepo1.Create(context.Background(), project); err != nil {
		t.Fatalf("create project: %v", err)
	}

	dep := &deployments.Deployment{
		ID:            "22222222-2222-2222-2222-222222222222",
		ProjectID:     project.ID,
		Revision:      "rev-restart",
		Image:         "nebula/pg-source:v1",
		DesiredState:  "RUNNING",
		Status:        deployments.StatusRunning,
		Stage:         "RUNNING",
		InstanceCount: 1,
		Env:           map[string]string{"APP_ENV": "prod"},
	}
	if err := depRepo1.Create(context.Background(), dep); err != nil {
		t.Fatalf("create deployment (epoch 1): %v", err)
	}

	inst := &deployments.Instance{
		ID:           "33333333-3333-3333-3333-333333333333",
		DeploymentID: dep.ID,
		WorkerID:     w.ID,
		InstanceKey:  "inst-pg-key-1",
		Status:       "RUNNING",
	}
	if err := instRepo1.Create(context.Background(), inst); err != nil {
		t.Fatalf("create instance (epoch 1): %v", err)
	}

	mockFactory.AddContainer(w.ID, &proto.ContainerInfo{
		InstanceId:  inst.InstanceKey,
		ContainerId: "mock-ctr-pg-1",
		Image:       dep.Image,
		Status:      "running",
		Labels: map[string]string{
			"nebula.instance_id":   inst.InstanceKey,
			"nebula.deployment_id": dep.ID,
		},
	})

	// Initial state converges with zero actions.
	initial, err := reconcile.NewReconciler(reg1, depRepo1, instRepo1,
		scheduler.NewScheduler(reg1, instRepo1.CountByWorkerForDeployment, log), mockFactory, log).ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}
	if !initial.IsZero() {
		t.Fatalf("expected zero actions before drift, got %+v", initial)
	}

	// While the CP is "down": a managed container disappears and a foreign
	// unmanaged container appears.
	mockFactory.RemoveContainer(w.ID, "mock-ctr-pg-1")
	mockFactory.AddContainer(w.ID, &proto.ContainerInfo{
		InstanceId:  "",
		ContainerId: "ctr-foreign-db-2",
		Image:       "postgres:16",
		Status:      "running",
		Labels:      map[string]string{"env": "local"},
	})

	// -------------------------------------------------------------
	// Epoch 2: CP restart. Everything must be reloaded from Postgres.
	// -------------------------------------------------------------
	pool2, err := pgxpool.New(context.Background(), target.String())
	if err != nil {
		t.Fatalf("pool2: %v", err)
	}
	defer pool2.Close()

	depRepo2 := storage.NewPostgresDeploymentRepository(pool2)
	instRepo2 := storage.NewPostgresInstanceRepository(pool2)
	workerRepo2 := storage.NewPostgresWorkerRepository(pool2)

	// Worker + desired state reloaded purely from Postgres.
	reg2 := workers.NewRegistry(workerRepo2, log)
	if got := len(reg2.List()); got != 1 {
		t.Fatalf("DA-01 / G-24 failure: expected 1 worker reloaded from Postgres, got %d", got)
	}

	deps, err := depRepo2.List(context.Background(), "")
	if err != nil || len(deps) != 1 {
		t.Fatalf("DA-01 / G-24 failure: expected 1 deployment reloaded from Postgres, n=%d err=%v", len(deps), err)
	}
	if deps[0].ID != dep.ID || deps[0].Image != dep.Image || deps[0].Env["APP_ENV"] != "prod" {
		t.Fatalf("DA-01 / G-24 failure: desired deployment misloaded from Postgres: %+v", deps[0])
	}

	// Startup reconcile happens before scheduling resumes (G-04).
	reconciler2 := reconcile.NewReconciler(reg2, depRepo2, instRepo2,
		scheduler.NewScheduler(reg2, instRepo2.CountByWorkerForDeployment, log), mockFactory, log)
	actions, err := reconciler2.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("restart reconcile failed: %v", err)
	}
	if actions.RecreatedCount != 1 {
		t.Fatalf("G-04 failure: expected 1 recreated container on restart reconcile, got %d", actions.RecreatedCount)
	}
	if actions.FlaggedCount != 1 || actions.StoppedCount != 0 {
		t.Fatalf("G-04 failure: expected 1 flagged unknown + 0 stopped, got %+v", actions)
	}

	// Desired == Observed after convergence; instance record back to RUNNING in Postgres.
	loadedWorker := reg2.List()[0]
	client, _ := mockFactory.GetClient(context.Background(), loadedWorker)
	list, _ := client.ListContainers(context.Background(), &proto.ListContainersRequest{})
	found := false
	for _, c := range list.Containers {
		if c.InstanceId == inst.InstanceKey && c.Status == "running" {
			found = true
		}
	}
	if !found {
		t.Fatalf("G-04 failure: repaired container not found on worker after restart")
	}

	updatedInst, err := instRepo2.GetByInstanceKey(context.Background(), inst.InstanceKey)
	if err != nil || updatedInst.Status != "RUNNING" {
		t.Fatalf("G-24 failure: instance in Postgres should be RUNNING, got %q err=%v", updatedInst.Status, err)
	}

	// Idempotent second pass over the same Postgres-sourced state performs no
	// mutating actions (DA-04 / G-02). Re-flagging a persistently-present unknown
	// container is a non-mutating audit action and is explicitly excluded.
	again, err := reconciler2.ReconcileOnce(context.Background())
	if err != nil || again.TotalActions() != 0 || len(again.Errors) != 0 {
		t.Fatalf("G-02 failure over Postgres: second pass must perform zero mutating actions, got %+v err=%v", again, err)
	}
	if again.StoppedCount != 0 || again.RecreatedCount != 0 {
		t.Fatalf("G-02 failure over Postgres: unexpected mutations on second pass: %+v", again)
	}

	t.Logf("CP-01..06 (G-04) + DA-01 (G-24) PASSED over real Postgres: restart reloaded desired state, repaired drift, flagged unknown, converged idempotently")
}
