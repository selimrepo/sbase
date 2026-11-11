-- Tenant boundaries and resource-level grants.
CREATE TABLE tenants (
    id UUID PRIMARY KEY,
    slug VARCHAR(80) NOT NULL UNIQUE,
    display_name VARCHAR(160) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','SUSPENDED','DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO tenants(id, slug, display_name)
VALUES ('00000000-0000-0000-0000-000000000001', 'legacy-default', 'Legacy default tenant')
ON CONFLICT (id) DO NOTHING;

ALTER TABLE datasets ADD COLUMN tenant_id UUID;
ALTER TABLE datasets ADD COLUMN owner_subject VARCHAR(200);
UPDATE datasets SET tenant_id='00000000-0000-0000-0000-000000000001', owner_subject='system';
ALTER TABLE datasets ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE datasets ALTER COLUMN owner_subject SET NOT NULL;
ALTER TABLE datasets ADD CONSTRAINT fk_datasets_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id);
ALTER TABLE datasets DROP CONSTRAINT uk_datasets_namespace_table;
ALTER TABLE datasets ADD CONSTRAINT uk_datasets_tenant_namespace_table UNIQUE (tenant_id, namespace, table_name);
CREATE INDEX idx_datasets_tenant_updated ON datasets(tenant_id, updated_at DESC);

ALTER TABLE pipelines ADD COLUMN tenant_id UUID;
ALTER TABLE pipelines ADD COLUMN owner_subject VARCHAR(200);
UPDATE pipelines SET tenant_id='00000000-0000-0000-0000-000000000001', owner_subject='system';
ALTER TABLE pipelines ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE pipelines ALTER COLUMN owner_subject SET NOT NULL;
ALTER TABLE pipelines ADD CONSTRAINT fk_pipelines_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id);
CREATE INDEX idx_pipelines_tenant_updated ON pipelines(tenant_id, updated_at DESC);

ALTER TABLE pipeline_runs ADD COLUMN tenant_id UUID;
ALTER TABLE pipeline_runs ADD COLUMN submitted_by_subject VARCHAR(200);
UPDATE pipeline_runs r SET tenant_id=p.tenant_id, submitted_by_subject=COALESCE(r.submitted_by,'system')
FROM pipelines p WHERE p.id=r.pipeline_id;
ALTER TABLE pipeline_runs ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE pipeline_runs ALTER COLUMN submitted_by_subject SET NOT NULL;
ALTER TABLE pipeline_runs ADD CONSTRAINT fk_pipeline_runs_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id);
CREATE INDEX idx_pipeline_runs_tenant_status ON pipeline_runs(tenant_id, status, submitted_at DESC);

CREATE TABLE resource_access_grants (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    resource_type VARCHAR(40) NOT NULL CHECK (resource_type IN ('DATASET','PIPELINE')),
    resource_id UUID NOT NULL,
    subject VARCHAR(200) NOT NULL,
    permission VARCHAR(12) NOT NULL CHECK (permission IN ('READ','WRITE','ADMIN')),
    granted_by VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, resource_type, resource_id, subject)
);
CREATE INDEX idx_resource_access_subject ON resource_access_grants(tenant_id, subject, resource_type, resource_id);

CREATE TABLE audit_events (
    id BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tenant_id UUID REFERENCES tenants(id),
    subject VARCHAR(200) NOT NULL,
    action VARCHAR(120) NOT NULL,
    resource_type VARCHAR(60),
    resource_id VARCHAR(160),
    outcome VARCHAR(20) NOT NULL,
    request_id VARCHAR(100),
    source_ip INET,
    user_agent TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX idx_audit_events_tenant_time ON audit_events(tenant_id, occurred_at DESC);
CREATE INDEX idx_audit_events_subject_time ON audit_events(subject, occurred_at DESC);

CREATE TABLE api_rate_limit_buckets (
    bucket_key VARCHAR(240) PRIMARY KEY,
    tokens DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
