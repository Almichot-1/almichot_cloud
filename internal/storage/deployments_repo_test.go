package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/projects"
)

// BLD-07 (Postgres half): a deployment's image digest is durably persisted and
// round-trips through the Postgres DeploymentRepository, gated by a projects FK.
func TestPostgresDeployment_DigestPersistedAndRoundTrips(t *testing.T) {
	admin, u := adminConn(t)
	defer admin.Close(context.Background())

	dbName, targetURL := createThrowawayDB(t, admin, u)
	defer dropDB(t, admin, dbName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, targetURL)
	if err != nil {
		t.Fatalf("pool for throwaway db: %v", err)
	}
	defer pool.Close()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	projectRepo := NewPostgresProjectRepository(pool)
	depRepo := NewPostgresDeploymentRepository(pool)

	// 1. A project row is required by the deployments.project_id FK.
	project := &projects.Project{Name: "digest-app", Description: "e2e", RepoURL: "https://example.com/repo"}
	if err := projectRepo.Create(ctx, project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if project.ID == "" {
		t.Fatal("expected Postgres to assign a project UUID")
	}

	// 2. Create a deployment carrying the verified image digest.
	const digest = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	dep := &deployments.Deployment{
		ID:            "11111111-1111-1111-1111-111111111111",
		ProjectID:     project.ID,
		Revision:      "rev-1",
		Image:         "nebula/digest-app:abcdef01",
		ImageDigest:   digest,
		DesiredState:  "RUNNING",
		Status:        deployments.StatusRunning,
		Stage:         "RUNNING",
		InstanceCount: 1,
		Env:           map[string]string{"APP_ENV": "production"},
		Labels:        map[string]string{"team": "platform"},
	}
	if err := depRepo.Create(ctx, dep); err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	// 3. Round-trip through Postgres.
	got, err := depRepo.GetByID(ctx, dep.ID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got.ImageDigest != digest {
		t.Errorf("digest round-trip: got %q, want %q", got.ImageDigest, digest)
	}
	if got.Image != dep.Image {
		t.Errorf("image round-trip: got %q, want %q", got.Image, dep.Image)
	}
	if got.Status != deployments.StatusRunning {
		t.Errorf("status round-trip: got %q, want RUNNING", got.Status)
	}
	if got.Env["APP_ENV"] != "production" {
		t.Errorf("env round-trip: got %v", got.Env)
	}

	// 4. Listing by project ID also returns the digest.
	list, err := depRepo.List(ctx, project.ID)
	if err != nil || len(list) != 1 {
		t.Fatalf("list deployments by project: n=%d err=%v", len(list), err)
	}
	if list[0].ImageDigest != digest {
		t.Errorf("list digest: got %q", list[0].ImageDigest)
	}
}
