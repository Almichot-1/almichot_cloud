-- 0002_secrets.sql
-- Envelope-encrypted secrets table (schema only for Phase 0; crypto lands in Phase 8).
-- Plane for the secrets/ package: ciphertext + key version, never plaintext.

CREATE TABLE IF NOT EXISTS secrets (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID REFERENCES deployments(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    ciphertext    BYTEA NOT NULL,
    key_version   INT NOT NULL DEFAULT 1,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (deployment_id, name)
);

CREATE INDEX IF NOT EXISTS idx_secrets_deployment_id ON secrets(deployment_id);
