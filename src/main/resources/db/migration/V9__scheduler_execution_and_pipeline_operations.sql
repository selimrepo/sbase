-- Scheduler lease/retry state required by the durable distributed worker.
ALTER TABLE scheduled_jobs
    ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(80),
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 4,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS misfire_grace_seconds INTEGER NOT NULL DEFAULT 60;
ALTER TABLE scheduled_jobs
    ADD CONSTRAINT scheduled_jobs_attempts_check CHECK (max_attempts BETWEEN 1 AND 20 AND attempt_count >= 0);
ALTER TABLE scheduled_jobs
    ADD CONSTRAINT scheduled_jobs_misfire_grace_check CHECK (misfire_grace_seconds BETWEEN 0 AND 604800);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_lease_due
    ON scheduled_jobs(enabled, next_run_at, lease_until);

ALTER TABLE schedule_runs
    ADD COLUMN IF NOT EXISTS handled_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_schedule_runs_unhandled
    ON schedule_runs(outcome_recorded_at, handled_at)
    WHERE handled_at IS NULL;

CREATE TABLE IF NOT EXISTS pipeline_step_runs (
    id UUID PRIMARY KEY,
    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    step_id VARCHAR(160) NOT NULL,
    step_type VARCHAR(80) NOT NULL,
    status VARCHAR(24) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT,
    records_processed BIGINT NOT NULL DEFAULT 0,
    output_schema_json TEXT,
    error_message TEXT,
    attempt INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(pipeline_run_id, step_id, attempt)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_step_runs_history
    ON pipeline_step_runs(pipeline_run_id, started_at);

ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS submitted_by VARCHAR(200) NOT NULL DEFAULT 'system';

CREATE TABLE IF NOT EXISTS bi_report_shares (
    report_id UUID NOT NULL REFERENCES bi_reports(id) ON DELETE CASCADE,
    subject VARCHAR(200) NOT NULL,
    permission VARCHAR(16) NOT NULL CHECK (permission IN ('READ','QUERY','EDIT')),
    granted_by VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(report_id, subject)
);
CREATE INDEX IF NOT EXISTS idx_bi_report_shares_subject ON bi_report_shares(subject, report_id);

CREATE TABLE IF NOT EXISTS bi_report_schedules (
    id UUID PRIMARY KEY,
    report_id UUID NOT NULL REFERENCES bi_reports(id) ON DELETE CASCADE,
    owner_subject VARCHAR(200) NOT NULL,
    cron_expression VARCHAR(120) NOT NULL,
    timezone VARCHAR(80) NOT NULL DEFAULT 'UTC',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    next_run_at TIMESTAMPTZ NOT NULL,
    last_run_at TIMESTAMPTZ,
    failure_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bi_report_schedules_due
    ON bi_report_schedules(enabled, next_run_at);

CREATE TABLE IF NOT EXISTS bi_report_schedule_runs (
    id UUID PRIMARY KEY,
    schedule_id UUID NOT NULL REFERENCES bi_report_schedules(id) ON DELETE CASCADE,
    status VARCHAR(24) NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    result_json TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_bi_report_schedule_runs
    ON bi_report_schedule_runs(schedule_id, started_at DESC);
