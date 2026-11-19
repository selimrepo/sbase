CREATE TABLE IF NOT EXISTS workflow_deployment (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    workflow_version INTEGER NOT NULL,
    slug VARCHAR(120) NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_concurrent_runs INTEGER NOT NULL DEFAULT 5 CHECK (max_concurrent_runs BETWEEN 1 AND 100),
    max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 20),
    invocation_count BIGINT NOT NULL DEFAULT 0 CHECK (invocation_count >= 0),
    last_invoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_workflow_deployment_slug CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,118}[a-z0-9]$'),
    CONSTRAINT fk_deployment_workflow_version
      FOREIGN KEY (workflow_id, workflow_version)
      REFERENCES workflow_version(workflow_id, version) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_workflow_deployment_workflow
    ON workflow_deployment(workflow_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_workflow_deployment_enabled
    ON workflow_deployment(enabled, slug);

CREATE INDEX IF NOT EXISTS ix_job_deployment_active
    ON job_execution(workflow_id, status)
    WHERE status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT');

COMMENT ON TABLE workflow_deployment IS
    'Published, version-pinned workflow services callable by external systems.';
