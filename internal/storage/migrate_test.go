package storage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var expectedTables = []string{
	"projects",
	"deployments",
	"workers",
	"instances",
	"secrets",
}

func adminConn(t *testing.T) (*pgx.Conn, *url.URL) {
	t.Helper()
	adminURL := os.Getenv("NEBULA_TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("NEBULA_TEST_DATABASE_URL not set; skipping Postgres-backed migration test")
	}

	u, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("parse admin URL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect to admin db: %v", err)
	}
	return conn, u
}

func createThrowawayDB(t *testing.T, admin *pgx.Conn, u *url.URL) (string, string) {
	t.Helper()
	dbName := fmt.Sprintf("nebula_test_%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create throwaway db: %v", err)
	}

	target := *u
	target.Path = "/" + dbName
	return dbName, target.String()
}

func dropDB(t *testing.T, admin *pgx.Conn, dbName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)"); err != nil {
		t.Logf("cleanup drop db %s: %v", dbName, err)
	}
}

func existingTables(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	rows, err := pool.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	found := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		found[name] = true
	}
	return found, rows.Err()
}

func TestDA00_MigrationsApplyCleanly(t *testing.T) {
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
		t.Fatalf("Migrate on empty DB returned error: %v", err)
	}

	tables, err := existingTables(ctx, pool)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	for _, want := range expectedTables {
		if !tables[want] {
			t.Errorf("expected table %q to exist after migration", want)
		}
	}
}

func TestDA00b_MigrationsIdempotent(t *testing.T) {
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
		t.Fatalf("first migration run: %v", err)
	}

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("re-running migration should be a no-op, got error: %v", err)
	}

	tables, err := existingTables(ctx, pool)
	if err != nil {
		t.Fatalf("list tables after re-run: %v", err)
	}
	for _, want := range expectedTables {
		if !tables[want] {
			t.Errorf("expected table %q to still exist after re-run", want)
		}
	}
}

