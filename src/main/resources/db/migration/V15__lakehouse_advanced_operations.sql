-- Advanced lakehouse operations: table-level grants, CDC offsets, replay and maintenance.
CREATE TABLE IF NOT EXISTS lakehouse_table_grant (
    id UUID PRIMARY KEY,
    catalog_name VARCHAR(128) NOT NULL DEFAULT 'sbase',
    namespace_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    principal_type VARCHAR(16) NOT NULL CHECK (principal_type IN ('USER','GROUP','WORKSPACE','SERVICE')),
    principal_id VARCHAR(200) NOT NULL,
    privilege VARCHAR(24) NOT NULL CHECK (privilege IN ('SELECT','INSERT','UPDATE','DELETE','ALTER','DROP','OWNERSHIP')),
    granted_by VARCHAR(200) NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    CONSTRAINT fk_lakehouse_table_grant_namespace
      FOREIGN KEY (catalog_name, namespace_name)
      REFERENCES lakehouse_namespace_owner(catalog_name, namespace_name) ON DELETE CASCADE,
    CONSTRAINT uq_lakehouse_table_grant
      UNIQUE (catalog_name, namespace_name, table_name, principal_type, principal_id, privilege)
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_table_grant_principal
 ON lakehouse_table_grant(principal_type, principal_id, catalog_name, namespace_name, table_name)
 WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS lakehouse_cdc_offset (
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES lakehouse_data_source(id) ON DELETE CASCADE,
    stream_name VARCHAR(200) NOT NULL,
    partition_key VARCHAR(500) NOT NULL,
    committed_offset JSONB NOT NULL,
    generation BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, source_id, stream_name, partition_key)
);

CREATE TABLE IF NOT EXISTS lakehouse_dead_letter (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    ingestion_run_id UUID NOT NULL REFERENCES lakehouse_ingestion_run(id) ON DELETE CASCADE,
    source_partition VARCHAR(500),
    source_offset JSONB,
    record_key TEXT,
    payload JSONB NOT NULL,
    error_class VARCHAR(300) NOT NULL,
    error_message TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','REPLAYING','RESOLVED','DISCARDED')),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_dead_letter_replay
 ON lakehouse_dead_letter(workspace_id, status, first_seen_at);

CREATE TABLE IF NOT EXISTS lakehouse_maintenance_policy (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    catalog_name VARCHAR(128) NOT NULL DEFAULT 'sbase',
    namespace_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128),
    operation VARCHAR(32) NOT NULL CHECK (operation IN ('COMPACT','EXPIRE_SNAPSHOTS','REMOVE_ORPHAN_FILES','REWRITE_MANIFESTS')),
    cron_expression VARCHAR(120) NOT NULL,
    retention_days INTEGER NOT NULL DEFAULT 7 CHECK (retention_days >= 1),
    orphan_older_than_hours INTEGER NOT NULL DEFAULT 72 CHECK (orphan_older_than_hours >= 24),
    enabled BOOLEAN NOT NULL DEFAULT true,
    last_started_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    created_by VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_lakehouse_maintenance_namespace
      FOREIGN KEY (catalog_name, namespace_name)
      REFERENCES lakehouse_namespace_owner(catalog_name, namespace_name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_maintenance_due
 ON lakehouse_maintenance_policy(next_run_at) WHERE enabled = true;

CREATE TABLE IF NOT EXISTS lakehouse_maintenance_run (
    id UUID PRIMARY KEY,
    policy_id UUID NOT NULL REFERENCES lakehouse_maintenance_policy(id) ON DELETE CASCADE,
    status VARCHAR(16) NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_maintenance_history
 ON lakehouse_maintenance_run(policy_id, started_at DESC);

CREATE TABLE IF NOT EXISTS lakehouse_schema_change (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    catalog_name VARCHAR(128) NOT NULL,
    namespace_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    schema_id INTEGER,
    change_type VARCHAR(24) NOT NULL CHECK (change_type IN ('ADD_COLUMN','DROP_COLUMN','RENAME_COLUMN','TYPE_WIDEN','REORDER','UNKNOWN')),
    change_payload JSONB NOT NULL,
    compatibility VARCHAR(16) NOT NULL CHECK (compatibility IN ('BACKWARD','FORWARD','FULL','BREAKING','UNKNOWN')),
    recorded_by VARCHAR(200) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_schema_history
 ON lakehouse_schema_change(workspace_id, catalog_name, namespace_name, table_name, recorded_at DESC);
