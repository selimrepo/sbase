CREATE TABLE pipeline_runs (
    id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL REFERENCES pipelines(id),
    pipeline_version INTEGER NOT NULL,
    trigger_type VARCHAR(24) NOT NULL,
    schedule_id UUID REFERENCES scheduled_jobs(id),
    status VARCHAR(24) NOT NULL,
    definition_snapshot TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    records_read BIGINT NOT NULL DEFAULT 0,
    records_written BIGINT NOT NULL DEFAULT 0,
    output_location TEXT,
    step_results_json TEXT NOT NULL DEFAULT '[]',
    result_summary TEXT,
    error_message TEXT
);
CREATE INDEX idx_pipeline_runs_pipeline_submitted ON pipeline_runs(pipeline_id, submitted_at DESC);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS last_run_id UUID REFERENCES pipeline_runs(id);
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS last_status VARCHAR(24);
