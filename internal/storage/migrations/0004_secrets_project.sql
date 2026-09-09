-- 0004_secrets_project.sql
-- Extend secrets and projects table for Phase 8 (SE-03, SE-04, SEC-01..04).

ALTER TABLE projects ADD COLUMN IF NOT EXISTS webhook_secret TEXT NOT NULL DEFAULT '';

ALTER TABLE secrets ADD COLUMN IF NOT EXISTS project_id UUID REFERENCES projects(id) ON DELETE CASCADE;
ALTER TABLE secrets ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;
ALTER TABLE secrets ALTER COLUMN deployment_id DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_secrets_project_id ON secrets(project_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_secrets_project_name_ver ON secrets(project_id, name, version) WHERE deployment_id IS NULL;
