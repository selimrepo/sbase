CREATE TABLE IF NOT EXISTS workflow_schedule (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    workflow_version INTEGER NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds BETWEEN 10 AND 31536000),
    max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 20),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_triggered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_schedule_workflow_version
      FOREIGN KEY (workflow_id, workflow_version)
      REFERENCES workflow_version(workflow_id, version) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_workflow_schedule_due
    ON workflow_schedule(next_run_at, id) WHERE enabled = TRUE;

CREATE TABLE IF NOT EXISTS workflow_audit_event (
    id BIGSERIAL PRIMARY KEY,
    workspace_id UUID REFERENCES workspace(id) ON DELETE SET NULL,
    workflow_id UUID REFERENCES workflow(id) ON DELETE SET NULL,
    event_type VARCHAR(80) NOT NULL,
    actor_id VARCHAR(200) NOT NULL DEFAULT 'system',
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_workflow_audit_workflow_time
    ON workflow_audit_event(workflow_id, created_at DESC);
