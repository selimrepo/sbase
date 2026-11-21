ALTER TABLE lakehouse_pipeline_attempt
 ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS ix_lakehouse_pipeline_attempt_due
 ON lakehouse_pipeline_attempt(next_attempt_at,submitted_at)
 WHERE status IN ('QUEUED','RETRY_WAIT');
