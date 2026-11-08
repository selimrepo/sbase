CREATE TABLE pipeline_runs (
    id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    pipeline_version INTEGER NOT NULL,
    status VARCHAR(24) NOT NULL CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    definition_snapshot TEXT NOT NULL,
    result_json TEXT,
    error_message TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    records_processed BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX idx_pipeline_runs_pipeline_submitted ON pipeline_runs(pipeline_id, submitted_at DESC);
CREATE INDEX idx_pipeline_runs_status_submitted ON pipeline_runs(status, submitted_at);
