CREATE TABLE IF NOT EXISTS lakehouse_ingestion_pipeline (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    source_id UUID NOT NULL REFERENCES lakehouse_data_source(id) ON DELETE RESTRICT,
    target_catalog VARCHAR(128) NOT NULL DEFAULT 'sbase',
    target_namespace VARCHAR(128) NOT NULL,
    target_table VARCHAR(128) NOT NULL,
    engine VARCHAR(16) NOT NULL CHECK (engine IN ('SPARK','FLINK')),
    mode VARCHAR(16) NOT NULL CHECK (mode IN ('BATCH','STREAMING','CDC')),
    configuration JSONB NOT NULL DEFAULT '{}'::jsonb,
    checkpoint_uri TEXT,
    dead_letter_enabled BOOLEAN NOT NULL DEFAULT true,
    max_retries INTEGER NOT NULL DEFAULT 5 CHECK (max_retries BETWEEN 0 AND 100),
    schedule_expression VARCHAR(120),
    enabled BOOLEAN NOT NULL DEFAULT true,
    active_version INTEGER NOT NULL DEFAULT 1,
    created_by VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id,name)
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_pipeline_workspace
 ON lakehouse_ingestion_pipeline(workspace_id,enabled,updated_at DESC);

CREATE TABLE IF NOT EXISTS lakehouse_pipeline_version (
    pipeline_id UUID NOT NULL REFERENCES lakehouse_ingestion_pipeline(id) ON DELETE CASCADE,
    version INTEGER NOT NULL CHECK (version>0),
    definition JSONB NOT NULL,
    checksum CHAR(64) NOT NULL,
    created_by VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id,version)
);

CREATE TABLE IF NOT EXISTS lakehouse_pipeline_attempt (
    id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL REFERENCES lakehouse_ingestion_pipeline(id) ON DELETE CASCADE,
    pipeline_version INTEGER NOT NULL,
    ingestion_run_id UUID REFERENCES lakehouse_ingestion_run(id) ON DELETE SET NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','RETRY_WAIT','CANCELLED')),
    idempotency_key VARCHAR(240) NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_class VARCHAR(300),
    error_message TEXT,
    UNIQUE (pipeline_id,idempotency_key),
    FOREIGN KEY (pipeline_id,pipeline_version)
      REFERENCES lakehouse_pipeline_version(pipeline_id,version)
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_pipeline_attempt_queue
 ON lakehouse_pipeline_attempt(status,submitted_at) WHERE status IN ('QUEUED','RETRY_WAIT');

CREATE TABLE IF NOT EXISTS lakehouse_maintenance_lease (
    policy_id UUID PRIMARY KEY REFERENCES lakehouse_maintenance_policy(id) ON DELETE CASCADE,
    lease_owner VARCHAR(200) NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    fencing_token BIGINT NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_maintenance_lease_until ON lakehouse_maintenance_lease(lease_until);
