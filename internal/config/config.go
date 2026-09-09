package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	App       AppConfig
	Postgres  PostgresConfig
	GRPC      GRPCConfig
	HTTP      HTTPConfig
	Worker    WorkerConfig
	Reconcile ReconcileConfig
	Auth      AuthConfig
	Secrets   SecretsConfig
}

type AuthConfig struct {
	Enabled    bool
	AdminToken string
}

type SecretsConfig struct {
	MasterKey  string
	KeyVersion int
}

type AppConfig struct {
	Env  string
	Name string
}

type PostgresConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	Database     string
	SSLMode      string
	MaxOpenConns int
	MaxIdleConns int
	MaxLifetime  time.Duration
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.User, p.Password, p.Host, p.Port, p.Database, p.SSLMode)
}

type GRPCConfig struct {
	ListenAddr string
}

// HTTPConfig defines the REST control plane HTTP listener.
type HTTPConfig struct {
	ListenAddr string
}

type WorkerConfig struct {
	ID             string
	HeartbeatInt   time.Duration
	ListenAddr     string
}

// ReconcileConfig tunes the reconciliation control loop (RCN-01, RCN-02).
type ReconcileConfig struct {
	// Interval is how often a full desired-vs-observed convergence pass runs.
	Interval time.Duration
	// Timeout bounds a single reconciliation pass; a repair that doesn't converge
	// within it is flagged, not retried silently.
	Timeout time.Duration
}

func Load() Config {
	return Config{
		App: AppConfig{
			Env:  getEnv("NEBULA_ENV", "development"),
			Name: getEnv("NEBULA_APP_NAME", "nebula"),
		},
		Postgres: PostgresConfig{
			Host:         getEnv("NEBULA_DB_HOST", "localhost"),
			Port:         getEnvInt("NEBULA_DB_PORT", 5432),
			User:         getEnv("NEBULA_DB_USER", "nebula"),
			Password:     getEnv("NEBULA_DB_PASSWORD", "nebula"),
			Database:     getEnv("NEBULA_DB_NAME", "nebula"),
			SSLMode:      getEnv("NEBULA_DB_SSLMODE", "disable"),
			MaxOpenConns: getEnvInt("NEBULA_DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns: getEnvInt("NEBULA_DB_MAX_IDLE_CONNS", 5),
			MaxLifetime:  getEnvDuration("NEBULA_DB_MAX_LIFETIME", 5*time.Minute),
		},
		GRPC: GRPCConfig{
			ListenAddr: getEnv("NEBULA_GRPC_ADDR", ":9090"),
		},
		HTTP: HTTPConfig{
			ListenAddr: getEnv("NEBULA_HTTP_ADDR", ":8081"),
		},
		Worker: WorkerConfig{
			ID:             getEnv("NEBULA_WORKER_ID", ""),
			HeartbeatInt:   getEnvDuration("NEBULA_HEARTBEAT_INTERVAL", 10*time.Second),
			ListenAddr:     getEnv("NEBULA_WORKER_ADDR", ":9091"),
		},
		Reconcile: ReconcileConfig{
			Interval: getEnvDuration("NEBULA_RECONCILE_INTERVAL", 5*time.Second),
			Timeout:  getEnvDuration("NEBULA_RECONCILE_TIMEOUT", 15*time.Second),
		},
		Auth: AuthConfig{
			Enabled:    getEnv("NEBULA_AUTH_ENABLED", "true") == "true",
			AdminToken: getEnv("NEBULA_ADMIN_TOKEN", "nebula-admin-token-secret-key-12345"),
		},
		Secrets: SecretsConfig{
			MasterKey:  getEnv("NEBULA_SECRETS_KEY", "nebula-32-byte-master-key-passphrase-01"),
			KeyVersion: getEnvInt("NEBULA_SECRETS_KEY_VERSION", 1),
		},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
