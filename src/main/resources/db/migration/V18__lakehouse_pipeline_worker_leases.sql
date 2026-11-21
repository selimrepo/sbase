ALTER TABLE lakehouse_pipeline_attempt
 ADD COLUMN IF NOT EXISTS lease_owner UUID,
 ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS ix_lakehouse_pipeline_lease
 ON lakehouse_pipeline_attempt(status,lease_until,submitted_at)
 WHERE status IN ('QUEUED','RETRY_WAIT','RUNNING');

ALTER TABLE lakehouse_ingestion_run
 ADD COLUMN IF NOT EXISTS pipeline_id UUID REFERENCES lakehouse_ingestion_pipeline(id) ON DELETE SET NULL,
 ADD COLUMN IF NOT EXISTS pipeline_attempt_id UUID REFERENCES lakehouse_pipeline_attempt(id) ON DELETE SET NULL,
 ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_lakehouse_ingestion_pipeline_run
 ON lakehouse_ingestion_run(workspace_id,pipeline_id,submitted_at DESC);
