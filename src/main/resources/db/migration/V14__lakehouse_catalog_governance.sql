-- Workspace-scoped governance metadata for the Iceberg lakehouse.
-- Iceberg JDBC remains the source of truth for physical namespaces and tables;
-- these tables store SBase ownership, authorization, tags and lineage overlays.

CREATE TABLE IF NOT EXISTS lakehouse_namespace_owner (
    catalog_name VARCHAR(128) NOT NULL DEFAULT 'sbase',
    namespace_name VARCHAR(128) NOT NULL,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    owner_principal VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_name, namespace_name),
    UNIQUE (workspace_id, namespace_name)
);

CREATE INDEX IF NOT EXISTS ix_lakehouse_namespace_workspace
    ON lakehouse_namespace_owner(workspace_id, namespace_name);

CREATE TABLE IF NOT EXISTS lakehouse_namespace_grant (
    id UUID PRIMARY KEY,
    catalog_name VARCHAR(128) NOT NULL DEFAULT 'sbase',
    namespace_name VARCHAR(128) NOT NULL,
    principal_type VARCHAR(16) NOT NULL CHECK (principal_type IN ('USER','GROUP','WORKSPACE','SERVICE')),
    principal_id VARCHAR(200) NOT NULL,
    privilege VARCHAR(24) NOT NULL CHECK (privilege IN ('USE','CREATE','SELECT','MODIFY','DROP','OWNERSHIP')),
    granted_by VARCHAR(200) NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    CONSTRAINT fk_lakehouse_grant_namespace
      FOREIGN KEY (catalog_name, namespace_name)
      REFERENCES lakehouse_namespace_owner(catalog_name, namespace_name) ON DELETE CASCADE,
    CONSTRAINT uq_lakehouse_namespace_grant
      UNIQUE (catalog_name, namespace_name, principal_type, principal_id, privilege)
);

CREATE INDEX IF NOT EXISTS ix_lakehouse_grant_principal
    ON lakehouse_namespace_grant(principal_type, principal_id, catalog_name, namespace_name)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS lakehouse_table_tag (
    catalog_name VARCHAR(128) NOT NULL DEFAULT 'sbase',
    namespace_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    tag_key VARCHAR(128) NOT NULL,
    tag_value VARCHAR(1000) NOT NULL,
    updated_by VARCHAR(200) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_name, namespace_name, table_name, tag_key),
    CONSTRAINT fk_lakehouse_tag_namespace
      FOREIGN KEY (catalog_name, namespace_name)
      REFERENCES lakehouse_namespace_owner(catalog_name, namespace_name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_lakehouse_table_tags_lookup
    ON lakehouse_table_tag(tag_key, tag_value);

CREATE TABLE IF NOT EXISTS lakehouse_lineage_edge (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    source_catalog VARCHAR(128) NOT NULL,
    source_namespace VARCHAR(128) NOT NULL,
    source_table VARCHAR(128) NOT NULL,
    target_catalog VARCHAR(128) NOT NULL,
    target_namespace VARCHAR(128) NOT NULL,
    target_table VARCHAR(128) NOT NULL,
    relation_type VARCHAR(24) NOT NULL CHECK (relation_type IN ('READS_FROM','WRITES_TO','DERIVED_FROM')),
    job_id UUID REFERENCES job_execution(id) ON DELETE SET NULL,
    run_id UUID,
    recorded_by VARCHAR(200) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    CHECK (
      (source_catalog, source_namespace, source_table) <>
      (target_catalog, target_namespace, target_table)
    )
);

CREATE INDEX IF NOT EXISTS ix_lakehouse_lineage_source
    ON lakehouse_lineage_edge(workspace_id, source_catalog, source_namespace, source_table, recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_lakehouse_lineage_target
    ON lakehouse_lineage_edge(workspace_id, target_catalog, target_namespace, target_table, recorded_at DESC);

CREATE TABLE IF NOT EXISTS lakehouse_data_source (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    name VARCHAR(200) NOT NULL,
    source_type VARCHAR(24) NOT NULL CHECK (source_type IN ('S3','JDBC','KAFKA','CASSANDRA','SOLR','POSTGRESQL')),
    configuration JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref VARCHAR(500),
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_by VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

CREATE TABLE IF NOT EXISTS lakehouse_ingestion_run (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    data_source_id UUID REFERENCES lakehouse_data_source(id) ON DELETE SET NULL,
    target_catalog VARCHAR(128) NOT NULL DEFAULT 'sbase',
    target_namespace VARCHAR(128) NOT NULL,
    target_table VARCHAR(128) NOT NULL,
    engine VARCHAR(16) NOT NULL CHECK (engine IN ('SPARK','FLINK')),
    status VARCHAR(20) NOT NULL CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    checkpoint_or_offset JSONB NOT NULL DEFAULT '{}'::jsonb,
    rows_read BIGINT NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_written BIGINT NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    error_message TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_lakehouse_ingestion_workspace_status
    ON lakehouse_ingestion_run(workspace_id, status, submitted_at DESC);

COMMENT ON TABLE lakehouse_namespace_owner IS 'SBase workspace ownership overlay for namespaces in the shared Iceberg JDBC catalog.';
COMMENT ON TABLE lakehouse_namespace_grant IS 'Workspace-aware namespace privileges; enforcement is performed by the SBase control plane.';
COMMENT ON TABLE lakehouse_table_tag IS 'Governance tags such as PII, retention class, domain and certification.';
COMMENT ON TABLE lakehouse_lineage_edge IS 'Table-level lineage edges emitted by Spark/Flink ingestion and workflow runs.';
COMMENT ON TABLE lakehouse_data_source IS 'Workspace-scoped connector definitions; credentials are external secret references only.';
COMMENT ON TABLE lakehouse_ingestion_run IS 'Durable ingestion execution metadata and replay/checkpoint positions.';
