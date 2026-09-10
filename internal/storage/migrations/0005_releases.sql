-- 0005_releases.sql
-- Durable releases table recording immutable image reference, cryptographic digest, and supply-chain signature (§10.1, §19.1, §21.3).

CREATE TABLE IF NOT EXISTS releases (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id    UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    deployment_id UUID REFERENCES deployments(id) ON DELETE SET NULL,
    version       TEXT NOT NULL DEFAULT '',
    image_ref     TEXT NOT NULL,
    image_digest  TEXT NOT NULL,
    signature     TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_releases_project_id ON releases(project_id);
CREATE INDEX IF NOT EXISTS idx_releases_image_digest ON releases(image_digest);
