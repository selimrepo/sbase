CREATE TABLE catalog_schema_versions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    table_asset_id UUID NOT NULL,
    schema_fingerprint VARCHAR(64) NOT NULL,
    schema_json JSONB NOT NULL CHECK (jsonb_typeof(schema_json)='array'),
    source_ref VARCHAR(512),
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by VARCHAR(200) NOT NULL,
    CONSTRAINT fk_catalog_schema_version_asset FOREIGN KEY (tenant_id,table_asset_id)
      REFERENCES catalog_assets(tenant_id,id) ON DELETE CASCADE,
    CONSTRAINT uq_catalog_schema_version UNIQUE (tenant_id,table_asset_id,schema_fingerprint)
);
CREATE INDEX idx_catalog_schema_versions_history
 ON catalog_schema_versions(tenant_id,table_asset_id,observed_at DESC);
