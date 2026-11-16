ALTER TABLE job_execution
 ADD COLUMN IF NOT EXISTS lease_owner UUID,
 ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 5,
 ADD COLUMN IF NOT EXISTS result JSONB,
 ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE job_execution ADD CONSTRAINT ck_job_max_attempts CHECK (max_attempts > 0);
CREATE INDEX IF NOT EXISTS ix_job_claimable ON job_execution(status,next_run_at,submitted_at) WHERE status IN ('QUEUED','RETRY_WAIT');
CREATE INDEX IF NOT EXISTS ix_job_expired_lease ON job_execution(lease_expires_at) WHERE status='RUNNING';
CREATE TABLE IF NOT EXISTS job_execution_event (
 id BIGSERIAL PRIMARY KEY, job_id UUID NOT NULL REFERENCES job_execution(id) ON DELETE CASCADE,
 event_type VARCHAR(48) NOT NULL, attempt INTEGER NOT NULL, details JSONB NOT NULL DEFAULT '{}'::jsonb,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_job_event_job_time ON job_execution_event(job_id,created_at);
CREATE TABLE IF NOT EXISTS workflow_node_run (
 job_id UUID NOT NULL REFERENCES job_execution(id) ON DELETE CASCADE, node_id VARCHAR(160) NOT NULL,
 status VARCHAR(24) NOT NULL CHECK(status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','SKIPPED')),
 started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, output JSONB, error_message TEXT,
 PRIMARY KEY(job_id,node_id)
);
