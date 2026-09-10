-- 0006_events.sql
-- Operational events and important lifecycle transitions (§19.1, §22, §27.2).

CREATE TABLE IF NOT EXISTS events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id    UUID REFERENCES projects(id) ON DELETE CASCADE,
    deployment_id UUID REFERENCES deployments(id) ON DELETE CASCADE,
    event_type    TEXT NOT NULL,
    message       TEXT NOT NULL,
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_project_id ON events(project_id);
CREATE INDEX IF NOT EXISTS idx_events_deployment_id ON events(deployment_id);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
