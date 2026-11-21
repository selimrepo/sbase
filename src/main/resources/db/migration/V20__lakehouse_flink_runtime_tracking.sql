ALTER TABLE lakehouse_pipeline_attempt
 ADD COLUMN IF NOT EXISTS runtime_job_id VARCHAR(64),
 ADD COLUMN IF NOT EXISTS result JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE lakehouse_ingestion_run
 ADD COLUMN IF NOT EXISTS flink_job_id VARCHAR(64);
CREATE INDEX IF NOT EXISTS ix_lakehouse_pipeline_flink_monitor
 ON lakehouse_pipeline_attempt(status,runtime_job_id)
 WHERE status='RUNNING' AND runtime_job_id IS NOT NULL;
