-- 0004_secrets_project.down.sql
-- Revert Phase 8 project secrets and webhook extensions.

DROP INDEX IF EXISTS uq_secrets_project_name_ver;
DROP INDEX IF EXISTS idx_secrets_project_id;

ALTER TABLE secrets DROP COLUMN IF EXISTS version;
ALTER TABLE secrets DROP COLUMN IF EXISTS project_id;
ALTER TABLE projects DROP COLUMN IF EXISTS webhook_secret;
