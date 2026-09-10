# Nebula Platform — Makefile
# ──────────────────────────────────────────────────────────────────────────────
# Targets:
#   make test          — run the in-memory gate suite (fast, no external deps)
#   make test-realinfra — run the real-infra gate suite (requires Docker)
#   make gate-lint     — run the banned-symbol lint over real-infra gate files
#   make build         — build all cmd binaries
#   make vet           — run go vet

.PHONY: test test-realinfra gate-lint build vet all

# ── Default: run the standard in-memory logic suite ──────────────────────────
test:
	go test -v -run "TestMVP_GateRun_G01_to_G28" ./tests/gate/ -timeout 120s

# ── Real-infra gate suite (Docker required) ───────────────────────────────────
# Requires: Docker daemon running, pg_dump/pg_restore on PATH.
# Tests skip gracefully if prerequisites are missing.
# Timeout is generous to allow for Postgres lock expiry (30-120s) and pg_dump.
test-realinfra:
	go test -v -tags realinfra -run "TestGate_.*_RealInfra|TestGate_ZZ" \
		./tests/gate/ -timeout 300s

# ── Guardrail 2: Banned-symbol lint ──────────────────────────────────────────
# Fails if any banned mock type appears inside the real-infra gate functions
# (G-26, G-34, G-36, G-37, G-43, G-44, G-46).
# Run this before running test-realinfra.
gate-lint:
	@echo "Running gate mock banned-symbol lint..."
	@bash scripts/lint-gate-mocks.sh

# ── Combined gate validation (lint then real-infra test) ──────────────────────
gate-validate: gate-lint test-realinfra

# ── Build all cmd binaries ────────────────────────────────────────────────────
build:
	go build ./cmd/control-plane/...
	go build ./cmd/worker-agent/...
	go build ./cmd/nebula-cli/...
	go build ./cmd/gate-registry-worker/...

# ── go vet ───────────────────────────────────────────────────────────────────
vet:
	go vet ./...

# ── All checks ────────────────────────────────────────────────────────────────
all: vet build test gate-lint
