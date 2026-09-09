package main

import (
	"os"

	"github.com/nebula/nebula/internal/config"
	"github.com/nebula/nebula/internal/observability"
)

func main() {
	cfg := config.Load()
	log := observability.New(cfg.App.Env).With().Str("component", "nebula-cli").Logger()

	log.Info().Str("env", cfg.App.Env).Msg("nebula-cli starting")

	log.Info().Msg("token/auth not yet implemented")
	// Thin client of the HTTP API — populated in later phases.
	os.Exit(0)
}
