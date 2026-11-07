ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(160);
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 5;
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_lease ON scheduled_jobs(enabled,next_run_at,lease_until);
ALTER TABLE ingestion_connectors ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(200);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_runs_idempotency ON ingestion_runs(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS model_artifacts(id UUID PRIMARY KEY,model_version_id UUID NOT NULL REFERENCES model_versions(id) ON DELETE CASCADE,object_key TEXT NOT NULL,sha256 CHAR(64) NOT NULL,size_bytes BIGINT NOT NULL,content_type VARCHAR(160) NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),UNIQUE(model_version_id,object_key));
CREATE TABLE IF NOT EXISTS feature_materializations(id UUID PRIMARY KEY,feature_set_id UUID NOT NULL REFERENCES feature_sets(id) ON DELETE CASCADE,version INTEGER NOT NULL,output_uri TEXT NOT NULL,status VARCHAR(24) NOT NULL,started_at TIMESTAMPTZ NOT NULL,finished_at TIMESTAMPTZ,error_message TEXT);
CREATE TABLE IF NOT EXISTS audit_events(id UUID PRIMARY KEY,subject VARCHAR(200) NOT NULL,action VARCHAR(120) NOT NULL,resource VARCHAR(240) NOT NULL,occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),details_json TEXT NOT NULL DEFAULT '{}');
INSERT INTO sbase_roles(id,name,description) VALUES
('00000000-0000-0000-0000-000000000001','ADMIN','Platform administrator'),
('00000000-0000-0000-0000-000000000002','DATA_ENGINEER','Manage ingestion and features'),
('00000000-0000-0000-0000-000000000003','DATA_SCIENTIST','Manage experiments and models'),
('00000000-0000-0000-0000-000000000004','BI_VIEWER','Read dashboards and BI')
ON CONFLICT(name) DO NOTHING;
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES
('platform:admin'),('ingestion:read'),('ingestion:write'),('features:read'),('features:write'),
('models:read'),('models:write'),('experiments:read'),('experiments:write'),('bi:read'),('bi:query')
) p(permission) WHERE r.name='ADMIN' ON CONFLICT DO NOTHING;
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES
('ingestion:read'),('ingestion:write'),('features:read'),('features:write')
) p(permission) WHERE r.name='DATA_ENGINEER' ON CONFLICT DO NOTHING;
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES
('models:read'),('models:write'),('experiments:read'),('experiments:write'),('features:read')
) p(permission) WHERE r.name='DATA_SCIENTIST' ON CONFLICT DO NOTHING;
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES
('bi:read'),('bi:query')
) p(permission) WHERE r.name='BI_VIEWER' ON CONFLICT DO NOTHING;
