ALTER TABLE lakehouse_dead_letter
    ADD COLUMN IF NOT EXISTS lease_token UUID,
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_replay_error TEXT;

CREATE INDEX IF NOT EXISTS ix_lakehouse_dead_letter_expired_lease
    ON lakehouse_dead_letter(workspace_id, lease_until)
    WHERE status='REPLAYING';

COMMENT ON COLUMN lakehouse_dead_letter.lease_token IS
    'Fencing token required to finish a replay claim; stale workers cannot resolve a newer claim.';
