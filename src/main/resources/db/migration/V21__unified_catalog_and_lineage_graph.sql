-- Unified, tenant-scoped catalog and lineage graph. All graph nodes share catalog_assets IDs.
CREATE TABLE catalog_assets (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    asset_type VARCHAR(32) NOT NULL CHECK (asset_type IN
      ('DATA_SOURCE','CONNECTOR','TABLE','COLUMN','PIPELINE','PIPELINE_STEP','FEATURE_SET','MODEL','REPORT','REPORT_FIELD','DASHBOARD','WIDGET')),
    external_key VARCHAR(512) NOT NULL,
    display_name VARCHAR(240) NOT NULL,
    namespace VARCHAR(160),
    physical_name VARCHAR(512),
    description TEXT,
    owner_subject VARCHAR(200) NOT NULL,
    owner_team VARCHAR(160),
    domain VARCHAR(120),
    classification VARCHAR(24) NOT NULL DEFAULT 'INTERNAL'
      CHECK (classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
    tags_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(tags_json)='array'),
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata_json)='object'),
    parent_id UUID,
    lifecycle_state VARCHAR(24) NOT NULL DEFAULT 'ACTIVE'
      CHECK (lifecycle_state IN ('ACTIVE','CERTIFIED','DEPRECATED','DRAFT')),
    schema_fingerprint VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uq_catalog_asset_external UNIQUE (tenant_id, asset_type, external_key),
    CONSTRAINT fk_catalog_asset_parent FOREIGN KEY (tenant_id,parent_id)
      REFERENCES catalog_assets(tenant_id,id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT uq_catalog_asset_tenant_id UNIQUE (tenant_id,id)
);
CREATE INDEX idx_catalog_assets_tenant_type_updated ON catalog_assets(tenant_id,asset_type,updated_at DESC);
CREATE INDEX idx_catalog_assets_tenant_parent ON catalog_assets(tenant_id,parent_id,asset_type);
CREATE INDEX idx_catalog_assets_tenant_owner ON catalog_assets(tenant_id,owner_subject);
CREATE INDEX idx_catalog_assets_tags ON catalog_assets USING GIN(tags_json);

CREATE TABLE catalog_columns (
    id UUID PRIMARY KEY REFERENCES catalog_assets(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL,
    table_asset_id UUID NOT NULL,
    ordinal_position INTEGER NOT NULL CHECK (ordinal_position > 0),
    data_type VARCHAR(160) NOT NULL,
    nullable BOOLEAN NOT NULL DEFAULT TRUE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    default_expression TEXT,
    generated_expression TEXT,
    CONSTRAINT fk_catalog_column_table FOREIGN KEY (tenant_id,table_asset_id)
      REFERENCES catalog_assets(tenant_id,id) ON DELETE CASCADE,
    CONSTRAINT uq_catalog_column_name UNIQUE (tenant_id,table_asset_id,id)
);
CREATE INDEX idx_catalog_columns_table ON catalog_columns(tenant_id,table_asset_id,ordinal_position);
CREATE UNIQUE INDEX uq_catalog_column_current_ordinal ON catalog_columns(tenant_id,table_asset_id,ordinal_position) WHERE is_current;

CREATE TABLE lineage_edges (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    from_asset_id UUID NOT NULL,
    to_asset_id UUID NOT NULL,
    edge_type VARCHAR(32) NOT NULL CHECK (edge_type IN
      ('CONTAINS','READS_FROM','DERIVES_FROM','WRITES_TO','CONSUMES','USES_COLUMN','USES_REPORT','DISPLAYS','PUBLISHES','DEPENDS_ON')),
    evidence_type VARCHAR(24) NOT NULL CHECK (evidence_type IN
      ('MIGRATION','PARSER','PIPELINE_DEFINITION','CONNECTOR_DISCOVERY','USER_DECLARED','RUNTIME_OBSERVED','BI_DEFINITION')),
    evidence_ref VARCHAR(512),
    expression_text TEXT,
    confidence NUMERIC(4,3) NOT NULL DEFAULT 1.000 CHECK (confidence BETWEEN 0 AND 1),
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_version BIGINT NOT NULL DEFAULT 1,
    run_id UUID,
    edge_fingerprint VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_lineage_from FOREIGN KEY (tenant_id,from_asset_id)
      REFERENCES catalog_assets(tenant_id,id) ON DELETE CASCADE,
    CONSTRAINT fk_lineage_to FOREIGN KEY (tenant_id,to_asset_id)
      REFERENCES catalog_assets(tenant_id,id) ON DELETE CASCADE,
    CONSTRAINT ck_lineage_not_self CHECK (from_asset_id <> to_asset_id),
    CONSTRAINT uq_lineage_fingerprint UNIQUE (tenant_id,edge_fingerprint)
);
CREATE INDEX idx_lineage_from ON lineage_edges(tenant_id,from_asset_id,edge_type);
CREATE INDEX idx_lineage_to ON lineage_edges(tenant_id,to_asset_id,edge_type);
CREATE INDEX idx_lineage_observed ON lineage_edges(tenant_id,observed_at DESC);

-- Backfill existing physical datasets and pipeline definitions into the common graph.
INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,namespace,physical_name,description,
 owner_subject,classification,metadata_json)
SELECT md5('dataset:'||d.id::text)::uuid,d.tenant_id,'TABLE',d.id::text,d.name,d.namespace,d.table_name,d.description,
 d.owner_subject,'INTERNAL',jsonb_build_object('legacyDatasetId',d.id,'sourceType',d.source_type)
FROM datasets d
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('pipeline:'||p.id::text)::uuid,p.tenant_id,'PIPELINE',p.id::text,p.name,p.description,p.owner_subject,
 jsonb_build_object('legacyPipelineId',p.id,'version',p.version,'enabled',p.enabled)
FROM pipelines p
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('bi-report:'||r.id::text)::uuid,r.tenant_id,'REPORT',r.id::text,r.name,r.description,r.owner_subject,
 jsonb_build_object('legacyReportId',r.id,'version',r.version)
FROM sbase_bi_reports r
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('bi-dashboard:'||d.id::text)::uuid,d.tenant_id,'DASHBOARD',d.id::text,d.name,d.description,d.owner_subject,
 jsonb_build_object('legacyDashboardId',d.id,'version',d.version,'shared',d.is_shared)
FROM sbase_bi_dashboards d
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,owner_subject,parent_id,metadata_json)
SELECT md5('bi-widget:'||w.id::text)::uuid,d.tenant_id,'WIDGET',w.id::text,w.title,d.owner_subject,
 md5('bi-dashboard:'||d.id::text)::uuid,jsonb_build_object('legacyWidgetId',w.id,'widgetType',w.widget_type)
FROM sbase_bi_widgets w JOIN sbase_bi_dashboards d ON d.id=w.dashboard_id
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
SELECT md5('lineage:dashboard-widget:'||w.id::text)::uuid,d.tenant_id,
 md5('bi-dashboard:'||d.id::text)::uuid,md5('bi-widget:'||w.id::text)::uuid,
 'CONTAINS','MIGRATION',w.id::text,md5('dashboard-widget:'||w.id::text)
FROM sbase_bi_widgets w JOIN sbase_bi_dashboards d ON d.id=w.dashboard_id
ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
SELECT md5('lineage:widget-report:'||w.id::text)::uuid,d.tenant_id,
 md5('bi-widget:'||w.id::text)::uuid,md5('bi-report:'||w.report_id::text)::uuid,
 'USES_REPORT','BI_DEFINITION',w.id::text,md5('widget-report:'||w.id::text)
FROM sbase_bi_widgets w JOIN sbase_bi_dashboards d ON d.id=w.dashboard_id
JOIN sbase_bi_reports r ON r.id=w.report_id AND r.tenant_id=d.tenant_id
WHERE w.report_id IS NOT NULL
ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,description,owner_subject,metadata_json)
SELECT md5('feature-set:'||f.id::text)::uuid,f.tenant_id,'FEATURE_SET',f.id::text,f.name,f.description,f.owner_subject,
 jsonb_build_object('legacyFeatureSetId',f.id,'version',f.version)
FROM feature_sets f
ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;

INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
SELECT md5('lineage:dataset-feature:'||f.id::text)::uuid,f.tenant_id,
 md5('dataset:'||f.dataset_id::text)::uuid,md5('feature-set:'||f.id::text)::uuid,
 'DERIVES_FROM','MIGRATION',f.id::text,md5('dataset-feature:'||f.id::text)
FROM feature_sets f JOIN datasets d ON d.id=f.dataset_id AND d.tenant_id=f.tenant_id
WHERE f.dataset_id IS NOT NULL
ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;

-- Preserve discovered columns as first-class catalog assets. Malformed legacy schema JSON is ignored safely.
DO $$
DECLARE d RECORD; item JSONB; schema_doc JSONB; col_name TEXT; col_type TEXT; pos INTEGER; col_id UUID; table_id UUID;
BEGIN
 FOR d IN SELECT id,tenant_id,schema_json,owner_subject FROM datasets LOOP
   table_id := md5('dataset:'||d.id::text)::uuid;
   BEGIN schema_doc := d.schema_json::jsonb;
   EXCEPTION WHEN others THEN schema_doc := '[]'::jsonb;
   END;
   IF jsonb_typeof(schema_doc) <> 'array' THEN CONTINUE; END IF;
   pos := 0;
   FOR item IN SELECT value FROM jsonb_array_elements(schema_doc) LOOP
     pos := pos + 1;
     col_name := COALESCE(NULLIF(item->>'name',''),NULLIF(item->>'columnName',''),NULLIF(item->>'field',''),'column_'||pos);
     col_type := COALESCE(NULLIF(item->>'type',''),NULLIF(item->>'dataType',''),'UNKNOWN');
     col_id := md5('dataset-column:'||d.id::text||':'||lower(col_name))::uuid;
     INSERT INTO catalog_assets(id,tenant_id,asset_type,external_key,display_name,physical_name,owner_subject,parent_id,metadata_json)
     VALUES(col_id,d.tenant_id,'COLUMN',d.id::text||':'||lower(col_name),col_name,col_name,d.owner_subject,table_id,
       jsonb_build_object('discoveredFrom','datasets.schema_json'))
     ON CONFLICT (tenant_id,asset_type,external_key) DO NOTHING;
     INSERT INTO catalog_columns(id,tenant_id,table_asset_id,ordinal_position,data_type,nullable)
     VALUES(col_id,d.tenant_id,table_id,pos,col_type,CASE WHEN item->'nullable' = 'false'::jsonb THEN FALSE ELSE TRUE END)
     ON CONFLICT (id) DO NOTHING;
     INSERT INTO lineage_edges(id,tenant_id,from_asset_id,to_asset_id,edge_type,evidence_type,evidence_ref,edge_fingerprint)
     VALUES(md5('lineage:table-column:'||d.id::text||':'||lower(col_name))::uuid,d.tenant_id,table_id,col_id,
       'CONTAINS','MIGRATION',d.id::text,md5('table-column:'||d.id::text||':'||lower(col_name)))
     ON CONFLICT (tenant_id,edge_fingerprint) DO NOTHING;
   END LOOP;
 END LOOP;
END $$;
