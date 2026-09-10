-- 0003_deployment_state.down.sql
-- Revert crash-recovery state columns from deployments table.

ALTER TABLE deployments DROP COLUMN IF EXISTS finished_at;
ALTER TABLE deployments DROP COLUMN IF EXISTS started_at;
ALTER TABLE deployments DROP COLUMN IF EXISTS scheduling_at;
ALTER TABLE deployments DROP COLUMN IF EXISTS building_at;
ALTER TABLE deployments DROP COLUMN IF EXISTS last_error;
ALTER TABLE deployments DROP COLUMN IF EXISTS attempts;
