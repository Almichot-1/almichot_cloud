-- 0007_autoscaling.sql
-- Replica management and autoscaling configuration (§18).

ALTER TABLE projects
    ADD COLUMN IF NOT EXISTS desired_replicas INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS min_replicas INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS max_replicas INT NOT NULL DEFAULT 10;

ALTER TABLE deployments
    ADD COLUMN IF NOT EXISTS desired_replicas INT NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS scaling_policies (
    id                         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id                 UUID NOT NULL UNIQUE REFERENCES projects(id) ON DELETE CASCADE,
    metric_type                TEXT NOT NULL,
    target_value               DOUBLE PRECISION NOT NULL,
    min_replicas               INT NOT NULL DEFAULT 1,
    max_replicas               INT NOT NULL DEFAULT 10,
    scale_up_cooldown_seconds   INT NOT NULL DEFAULT 60,
    scale_down_cooldown_seconds INT NOT NULL DEFAULT 300,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scaling_policies_project_id ON scaling_policies(project_id);
