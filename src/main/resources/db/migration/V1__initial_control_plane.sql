CREATE TABLE IF NOT EXISTS sbase_schema_version (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workspace (
    id UUID PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    active_version INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_version (
    workflow_id UUID NOT NULL REFERENCES workflow(id) ON DELETE CASCADE,
    version INTEGER NOT NULL CHECK (version > 0),
    definition JSONB NOT NULL,
    checksum CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, version)
);

ALTER TABLE workflow
    ADD CONSTRAINT fk_workflow_active_version
    FOREIGN KEY (id, active_version)
    REFERENCES workflow_version(workflow_id, version)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS job_execution (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    workflow_version INTEGER NOT NULL,
    status VARCHAR(24) NOT NULL CHECK (status IN
      ('QUEUED', 'SCHEDULED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'RETRY_WAIT')),
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    idempotency_key VARCHAR(200),
    correlation_id UUID NOT NULL,
    error_code VARCHAR(100),
    error_message TEXT,
    CONSTRAINT fk_job_workflow_version
      FOREIGN KEY (workflow_id, workflow_version)
      REFERENCES workflow_version(workflow_id, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_job_idempotency
    ON job_execution(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_job_status_submitted
    ON job_execution(status, submitted_at);
CREATE INDEX IF NOT EXISTS ix_job_workflow_submitted
    ON job_execution(workflow_id, submitted_at DESC);

CREATE TABLE IF NOT EXISTS transactional_outbox (
    id BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending
    ON transactional_outbox(created_at) WHERE published_at IS NULL;
