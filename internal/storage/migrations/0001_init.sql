-- 0001_init.sql
-- Core Nebula schema: projects, deployments, workers, instances.

-- CREATE EXTENSION IF NOT EXISTS "pgcrypto"; -- for gen_random_uuid() on PG < 13

CREATE TABLE IF NOT EXISTS projects (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    repo_url    TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workers (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_key   TEXT NOT NULL UNIQUE,
    hostname     TEXT NOT NULL DEFAULT '',
    ip_address   TEXT NOT NULL DEFAULT '',
    capacity     INT  NOT NULL DEFAULT 0,
    labels       JSONB NOT NULL DEFAULT '{}'::jsonb,
    state        TEXT NOT NULL DEFAULT 'UNREGISTERED',
    health       TEXT NOT NULL DEFAULT 'UNKNOWN',
    schedulable  BOOLEAN NOT NULL DEFAULT FALSE,
    last_beat_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS deployments (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id    UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    revision      TEXT NOT NULL DEFAULT '',
    image         TEXT NOT NULL DEFAULT '',
    image_digest  TEXT NOT NULL DEFAULT '',
    desired_state TEXT NOT NULL DEFAULT 'RUNNING',
    status        TEXT NOT NULL DEFAULT 'QUEUED',
    stage         TEXT NOT NULL DEFAULT 'QUEUED',
    instance_count INT NOT NULL DEFAULT 1,
    env           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS instances (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    worker_id     UUID REFERENCES workers(id) ON DELETE SET NULL,
    instance_key  TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL DEFAULT 'PENDING',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_deployments_project_id ON deployments(project_id);
CREATE INDEX IF NOT EXISTS idx_deployments_status     ON deployments(status);
CREATE INDEX IF NOT EXISTS idx_instances_deployment_id ON instances(deployment_id);
CREATE INDEX IF NOT EXISTS idx_instances_worker_id      ON instances(worker_id);
CREATE INDEX IF NOT EXISTS idx_workers_state            ON workers(state);
