CREATE TABLE datasets (
    id UUID PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    namespace VARCHAR(120) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    description TEXT,
    source_type VARCHAR(32) NOT NULL,
    schema_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_datasets_namespace_table UNIQUE (namespace, table_name)
);
CREATE INDEX idx_datasets_name ON datasets (name);
CREATE INDEX idx_datasets_updated_at ON datasets (updated_at DESC);

CREATE TABLE pipelines (
    id UUID PRIMARY KEY,
    name VARCHAR(160) NOT NULL,
    description TEXT,
    definition_json TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_pipelines_name ON pipelines (name);
CREATE INDEX idx_pipelines_updated_at ON pipelines (updated_at DESC);