CREATE TABLE mining_jobs (
    id UUID PRIMARY KEY,
    operation VARCHAR(40) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    output_table VARCHAR(255) NOT NULL,
    status VARCHAR(24) NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    result_summary TEXT,
    error_message TEXT
);
CREATE INDEX idx_mining_jobs_submitted_at ON mining_jobs (submitted_at DESC);
CREATE INDEX idx_mining_jobs_status ON mining_jobs (status);
