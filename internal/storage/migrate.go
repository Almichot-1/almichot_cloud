package storage

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies all embedded SQL migrations in order via the pool.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping before migrate: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") || strings.HasSuffix(e.Name(), ".down.sql") {
			continue
		}
		sql, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", e.Name(), err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("execute %s: %w", e.Name(), err)
		}
	}
	return nil
}

// Rollback reverses applied migrations in reverse numerical order up to the specified number of steps.
// If steps <= 0, it rolls back all migrations.
func Rollback(ctx context.Context, pool *pgxpool.Pool, steps int) error {
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping before rollback: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	var downEntries []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".down.sql") {
			downEntries = append(downEntries, e.Name())
		}
	}

	// Reverse order: 0004_...down.sql down to 0001_...down.sql
	sort.Slice(downEntries, func(i, j int) bool {
		return downEntries[i] > downEntries[j]
	})

	count := 0
	for _, name := range downEntries {
		if steps > 0 && count >= steps {
			break
		}
		sql, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("execute %s: %w", name, err)
		}
		count++
	}
	return nil
}

// RollbackAll rolls back all applied migrations in reverse order down to an empty schema.
func RollbackAll(ctx context.Context, pool *pgxpool.Pool) error {
	return Rollback(ctx, pool, 0)
}
