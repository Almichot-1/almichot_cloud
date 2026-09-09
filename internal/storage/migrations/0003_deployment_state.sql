-- 0003_deployment_state.sql
-- Explicit per-stage state columns for crash recovery (Phase 6, G-16..G-20).
-- The deployments table already carries `status` (lifecycle outcome) and `stage`
-- (current pipeline stage). This migration adds the crash-recovery support
-- columns that the state machine needs to resume an interrupted deployment.
-- Idempotent: uses ADD COLUMN IF NOT EXISTS.

ALTER TABLE deployments ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS building_at  TIMESTAMPTZ;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS scheduling_at TIMESTAMPTZ;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS started_at   TIMESTAMPTZ;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS finished_at  TIMESTAMPTZ;
