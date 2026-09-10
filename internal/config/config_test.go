package config

import (
	"testing"
	"time"
)

func clearNebulaEnv(t *testing.T) {
	t.Helper()
	keys := []string{
		"NEBULA_ENV", "NEBULA_APP_NAME",
		"NEBULA_DB_HOST", "NEBULA_DB_PORT", "NEBULA_DB_USER", "NEBULA_DB_PASSWORD",
		"NEBULA_DB_NAME", "NEBULA_DB_SSLMODE", "NEBULA_DB_MAX_OPEN_CONNS",
		"NEBULA_DB_MAX_IDLE_CONNS", "NEBULA_DB_MAX_LIFETIME",
		"NEBULA_GRPC_ADDR", "NEBULA_HTTP_ADDR",
		"NEBULA_WORKER_ID", "NEBULA_WORKER_ADDR", "NEBULA_HEARTBEAT_INTERVAL",
		"NEBULA_RECONCILE_INTERVAL", "NEBULA_RECONCILE_TIMEOUT",
	}
	for _, k := range keys {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearNebulaEnv(t)

	cfg := Load()

	if cfg.App.Env != "development" {
		t.Errorf("Env default: got %q, want %q", cfg.App.Env, "development")
	}
	if cfg.App.Name != "nebula" {
		t.Errorf("Name default: got %q, want %q", cfg.App.Name, "nebula")
	}
	if cfg.Postgres.Host != "localhost" {
		t.Errorf("PG host default: got %q, want %q", cfg.Postgres.Host, "localhost")
	}
	if cfg.Postgres.Port != 5432 {
		t.Errorf("PG port default: got %d, want %d", cfg.Postgres.Port, 5432)
	}
	if cfg.Postgres.User != "nebula" {
		t.Errorf("PG user default: got %q, want %q", cfg.Postgres.User, "nebula")
	}
	if cfg.Postgres.Password != "nebula" {
		t.Errorf("PG password default: got %q, want %q", cfg.Postgres.Password, "nebula")
	}
	if cfg.Postgres.Database != "nebula" {
		t.Errorf("PG db default: got %q, want %q", cfg.Postgres.Database, "nebula")
	}
	if cfg.Postgres.SSLMode != "disable" {
		t.Errorf("PG sslmode default: got %q, want %q", cfg.Postgres.SSLMode, "disable")
	}
	if cfg.Postgres.MaxOpenConns != 25 {
		t.Errorf("PG max open conns default: got %d, want %d", cfg.Postgres.MaxOpenConns, 25)
	}
	if cfg.Postgres.MaxIdleConns != 5 {
		t.Errorf("PG max idle conns default: got %d, want %d", cfg.Postgres.MaxIdleConns, 5)
	}
	if cfg.Postgres.MaxLifetime != 5*time.Minute {
		t.Errorf("PG max lifetime default: got %v, want %v", cfg.Postgres.MaxLifetime, 5*time.Minute)
	}
	if cfg.GRPC.ListenAddr != ":9090" {
		t.Errorf("gRPC addr default: got %q, want %q", cfg.GRPC.ListenAddr, ":9090")
	}
	if cfg.HTTP.ListenAddr != ":8081" {
		t.Errorf("HTTP addr default: got %q, want %q", cfg.HTTP.ListenAddr, ":8081")
	}
	if cfg.Worker.ID != "" {
		t.Errorf("worker id default: got %q, want %q", cfg.Worker.ID, "")
	}
	if cfg.Worker.HeartbeatInt != 10*time.Second {
		t.Errorf("heartbeat default: got %v, want %v", cfg.Worker.HeartbeatInt, 10*time.Second)
	}
	if cfg.Worker.ListenAddr != ":9091" {
		t.Errorf("worker addr default: got %q, want %q", cfg.Worker.ListenAddr, ":9091")
	}
	if cfg.Reconcile.Interval != 5*time.Second {
		t.Errorf("reconcile interval default: got %v, want %v", cfg.Reconcile.Interval, 5*time.Second)
	}
	if cfg.Reconcile.Timeout != 15*time.Second {
		t.Errorf("reconcile timeout default: got %v, want %v", cfg.Reconcile.Timeout, 15*time.Second)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearNebulaEnv(t)
	t.Setenv("NEBULA_ENV", "production")
	t.Setenv("NEBULA_DB_HOST", "db.internal")
	t.Setenv("NEBULA_DB_PORT", "6432")
	t.Setenv("NEBULA_DB_USER", "svc")
	t.Setenv("NEBULA_DB_PASSWORD", "s3cret")
	t.Setenv("NEBULA_DB_NAME", "catalog")
	t.Setenv("NEBULA_DB_SSLMODE", "require")
	t.Setenv("NEBULA_GRPC_ADDR", ":9999")
	t.Setenv("NEBULA_HTTP_ADDR", ":8888")
	t.Setenv("NEBULA_WORKER_ID", "worker-7")
	t.Setenv("NEBULA_WORKER_ADDR", ":7777")
	t.Setenv("NEBULA_HEARTBEAT_INTERVAL", "2s")
	t.Setenv("NEBULA_RECONCILE_INTERVAL", "3s")
	t.Setenv("NEBULA_RECONCILE_TIMEOUT", "45s")

	cfg := Load()

	if cfg.App.Env != "production" {
		t.Errorf("Env override: got %q", cfg.App.Env)
	}
	if cfg.Postgres.Host != "db.internal" {
		t.Errorf("PG host override: got %q", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 6432 {
		t.Errorf("PG port override: got %d", cfg.Postgres.Port)
	}
	if cfg.Postgres.User != "svc" || cfg.Postgres.Password != "s3cret" {
		t.Errorf("PG creds override: got %q/%q", cfg.Postgres.User, cfg.Postgres.Password)
	}
	if cfg.Postgres.Database != "catalog" {
		t.Errorf("PG db override: got %q", cfg.Postgres.Database)
	}
	if cfg.Postgres.SSLMode != "require" {
		t.Errorf("PG sslmode override: got %q", cfg.Postgres.SSLMode)
	}
	if cfg.GRPC.ListenAddr != ":9999" {
		t.Errorf("gRPC addr override: got %q", cfg.GRPC.ListenAddr)
	}
	if cfg.HTTP.ListenAddr != ":8888" {
		t.Errorf("HTTP addr override: got %q", cfg.HTTP.ListenAddr)
	}
	if cfg.Worker.ID != "worker-7" {
		t.Errorf("worker id override: got %q", cfg.Worker.ID)
	}
	if cfg.Worker.ListenAddr != ":7777" {
		t.Errorf("worker addr override: got %q", cfg.Worker.ListenAddr)
	}
	if cfg.Worker.HeartbeatInt != 2*time.Second {
		t.Errorf("heartbeat override: got %v", cfg.Worker.HeartbeatInt)
	}
	if cfg.Reconcile.Interval != 3*time.Second {
		t.Errorf("reconcile interval override: got %v", cfg.Reconcile.Interval)
	}
	if cfg.Reconcile.Timeout != 45*time.Second {
		t.Errorf("reconcile timeout override: got %v", cfg.Reconcile.Timeout)
	}
}

func TestLoadInvalidValuesFallBack(t *testing.T) {
	clearNebulaEnv(t)
	t.Setenv("NEBULA_DB_PORT", "not-a-number")
	t.Setenv("NEBULA_DB_MAX_OPEN_CONNS", "zzz")
	t.Setenv("NEBULA_DB_MAX_LIFETIME", "nonsense")
	t.Setenv("NEBULA_HEARTBEAT_INTERVAL", "garbage")
	t.Setenv("NEBULA_RECONCILE_INTERVAL", "garbage")
	t.Setenv("NEBULA_RECONCILE_TIMEOUT", "garbage")

	cfg := Load()

	if cfg.Postgres.Port != 5432 {
		t.Errorf("invalid PG port: got %d, want default 5432", cfg.Postgres.Port)
	}
	if cfg.Postgres.MaxOpenConns != 25 {
		t.Errorf("invalid max open conns: got %d, want default 25", cfg.Postgres.MaxOpenConns)
	}
	if cfg.Postgres.MaxLifetime != 5*time.Minute {
		t.Errorf("invalid max lifetime: got %v, want default 5m", cfg.Postgres.MaxLifetime)
	}
	if cfg.Worker.HeartbeatInt != 10*time.Second {
		t.Errorf("invalid heartbeat: got %v, want default 10s", cfg.Worker.HeartbeatInt)
	}
	if cfg.Reconcile.Interval != 5*time.Second {
		t.Errorf("invalid reconcile interval: got %v, want default 5s", cfg.Reconcile.Interval)
	}
	if cfg.Reconcile.Timeout != 15*time.Second {
		t.Errorf("invalid reconcile timeout: got %v, want default 15s", cfg.Reconcile.Timeout)
	}
}

func TestPostgresDSN(t *testing.T) {
	p := PostgresConfig{
		Host:     "dbhost",
		Port:     5433,
		User:     "alice",
		Password: "pw",
		Database: "nebula",
		SSLMode:  "disable",
	}
	dsn := p.DSN()
	want := "postgres://alice:pw@dbhost:5433/nebula?sslmode=disable"
	if dsn != want {
		t.Errorf("DSN: got %q, want %q", dsn, want)
	}
}
