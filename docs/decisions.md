# Nebula Architecture Decision Records (ADRs) & Scope Clarifications

This document formally records intentional architectural scope boundaries and deferred capabilities for the current Nebula MVP milestone. These decisions are conscious design trade-offs to prioritize distributed systems correctness, scheduler invariants, failure recovery, and zero-downtime release guarantees over presentation-layer features.

---

## ADR-001: Next.js Web Dashboard (§7.1)

* **Status:** Intentionally Deferred for Backend MVP Milestone
* **Context:**
  The architecture specification outlines a unified web console (§7.1) for deployment visualization and log tailing. The repository contains the Next.js 15 application shell in [`dashboard/`](../dashboard).
* **Decision:**
  The current MVP milestone is defined as **headless, API-first, and CLI-driven**.
  All control-plane capabilities, deployment orchestration, health state machines, and failovers are managed via the REST API and `nebula-cli`.
* **Evidence:**
  Gate G-45 explicitly validates that the complete end-to-end deployment lifecycle functions with zero web console dependency. Automated browser-based E2E verification (Playwright/Cypress) is deferred to the subsequent UI milestone.

---

## ADR-002: User Authentication & Interactive Session Flow (§21.1)

* **Status:** Satisfied via Scoped API Tokens; Interactive Login Flow Deferred
* **Context:**
  Section 21.1 requires authenticating dashboard/CLI users before granting project access.
* **Decision:**
  For the MVP milestone, authentication is implemented and enforced using cryptographically secure pre-shared project API tokens (`X-Nebula-Token` / `Bearer <token>`).
  Downstream authorization, RBAC, and cross-project isolation are strictly enforced:
  - Missing or invalid tokens return HTTP 401 Unauthorized.
  - Cross-tenant or unauthorized project access returns HTTP 403 Forbidden.
  - Verified by [`tests/security/auth_test.go`](../tests/security/auth_test.go).
  Interactive user sessions (`POST /v1/auth/login`, password hashing with Argon2, user account management, and OIDC/OAuth2 token exchanges) are deferred to the multi-user enterprise milestone.

---

## ADR-003: Raw Container Log Aggregation & Live Tail Pipeline (§22)

* **Status:** Intentionally Deferred for Current Milestone
* **Context:**
  Section 22 lists four observability signals: Metrics, Events, Tracing, and Logs.
* **Decision:**
  For the current milestone:
  - **Metrics**: Verified via Prometheus/StatsD metrics collection (Gate G-38).
  - **Tracing**: Verified via OpenTelemetry distributed trace propagation (Gate G-39).
  - **Events**: Verified via structured deployment and worker lifecycle events (Gate G-40).
  - **Structured Logging & Redaction**: Verified via structured JSON log emission with correlation IDs and sensitive credential sanitization ([`tests/security/secret_redaction_test.go`](../tests/security/secret_redaction_test.go)).
  The real-time streaming multiplexer for container stdout/stderr (streaming Docker daemon logs through the Worker Agent and Control Plane to `nebula logs -f`) is deferred to the Phase 3 observability pipeline upgrade.
