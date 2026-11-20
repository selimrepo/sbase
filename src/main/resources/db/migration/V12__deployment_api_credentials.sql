ALTER TABLE workflow_deployment
    ADD COLUMN IF NOT EXISTS api_key_hash VARCHAR(64),
    ADD COLUMN IF NOT EXISTS api_key_prefix VARCHAR(20),
    ADD COLUMN IF NOT EXISTS api_key_rotated_at TIMESTAMPTZ;

COMMENT ON COLUMN workflow_deployment.api_key_hash IS
    'SHA-256 digest of the one-time-issued deployment API credential; plaintext is never persisted.';
