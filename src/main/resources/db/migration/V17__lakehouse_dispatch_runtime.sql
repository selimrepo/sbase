-- Authenticated Lakehouse dispatch and scheduler execution state.
CREATE TABLE IF NOT EXISTS lakehouse_dispatch_job (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    job_type VARCHAR(24) NOT NULL CHECK (job_type IN ('INGESTION','MAINTENANCE','FLINK_STREAM')),
    resource_id UUID NOT NULL,
    resource_version INTEGER NOT NULL DEFAULT 1,
    idempotency_key VARCHAR(240) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'QUEUED'
      CHECK (status IN ('QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED')),
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner UUID,
    lease_until TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_class VARCHAR(300),
    error_message TEXT,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(workspace_id,idempotency_key)
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_dispatch_claim
 ON lakehouse_dispatch_job(status,available_at,submitted_at)
 WHERE status IN ('QUEUED','RETRY_WAIT','RUNNING');

CREATE TABLE IF NOT EXISTS lakehouse_maintenance_policy_audit (
    id BIGSERIAL PRIMARY KEY,
    policy_id UUID NOT NULL REFERENCES lakehouse_maintenance_policy(id) ON DELETE CASCADE,
    event_type VARCHAR(40) NOT NULL,
    fencing_token BIGINT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE lakehouse_maintenance_policy
 ADD COLUMN IF NOT EXISTS interval_seconds INTEGER NOT NULL DEFAULT 86400
 CHECK (interval_seconds BETWEEN 60 AND 31536000);
ALTER TABLE lakehouse_maintenance_policy
 ADD COLUMN IF NOT EXISTS lease_owner UUID;
ALTER TABLE lakehouse_maintenance_policy
 ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
ALTER TABLE lakehouse_maintenance_policy
 ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS ix_lakehouse_maintenance_due_v2
 ON lakehouse_maintenance_policy(next_run_at,lease_until)
 WHERE enabled=true;

UPDATE lakehouse_maintenance_policy SET next_run_at=now() WHERE enabled=true AND next_run_at IS NULL;
