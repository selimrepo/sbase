-- ML lifecycle, point-in-time feature history, governed BI queries and dashboard delivery.
CREATE TABLE IF NOT EXISTS sbase_ml_experiments (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, name VARCHAR(160) NOT NULL, description TEXT,
 owner_subject VARCHAR(200) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,name)
);
CREATE TABLE IF NOT EXISTS sbase_ml_runs (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, experiment_id UUID NOT NULL REFERENCES sbase_ml_experiments(id) ON DELETE CASCADE,
 run_name VARCHAR(160) NOT NULL, algorithm VARCHAR(80) NOT NULL, task_type VARCHAR(24) NOT NULL CHECK(task_type IN ('REGRESSION','CLASSIFICATION')),
 status VARCHAR(24) NOT NULL CHECK(status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
 parameters_json JSONB NOT NULL DEFAULT '{}'::jsonb, search_space_json JSONB NOT NULL DEFAULT '{}'::jsonb,
 metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb, best_parameters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
 dataset_table VARCHAR(256) NOT NULL, feature_columns_json JSONB NOT NULL, label_column VARCHAR(128) NOT NULL,
 cross_validation_folds INTEGER NOT NULL DEFAULT 1 CHECK(cross_validation_folds BETWEEN 1 AND 10),
 artifact_uri TEXT, error_message TEXT, submitted_by VARCHAR(200) NOT NULL,
 started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sbase_ml_runs_experiment ON sbase_ml_runs(tenant_id,experiment_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sbase_ml_runs_status ON sbase_ml_runs(status,created_at);
CREATE TABLE IF NOT EXISTS sbase_ml_models (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, name VARCHAR(160) NOT NULL, description TEXT,
 owner_subject VARCHAR(200) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,name)
);
CREATE TABLE IF NOT EXISTS sbase_ml_model_versions (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, model_id UUID NOT NULL REFERENCES sbase_ml_models(id) ON DELETE CASCADE,
 run_id UUID REFERENCES sbase_ml_runs(id) ON DELETE SET NULL, version INTEGER NOT NULL,
 artifact_uri TEXT NOT NULL, framework VARCHAR(80) NOT NULL, algorithm VARCHAR(80) NOT NULL,
 task_type VARCHAR(24) NOT NULL, metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
 signature_json JSONB NOT NULL DEFAULT '{}'::jsonb, stage VARCHAR(24) NOT NULL DEFAULT 'NONE'
 CHECK(stage IN ('NONE','STAGING','PRODUCTION','ARCHIVED')),
 created_by VARCHAR(200) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(model_id,version)
);
CREATE INDEX IF NOT EXISTS idx_sbase_ml_model_stage ON sbase_ml_model_versions(tenant_id,model_id,stage);
CREATE TABLE IF NOT EXISTS sbase_ml_deployments (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, model_version_id UUID NOT NULL REFERENCES sbase_ml_model_versions(id),
 deployment_name VARCHAR(160) NOT NULL, status VARCHAR(24) NOT NULL CHECK(status IN ('ACTIVE','STOPPED')),
 traffic_percent NUMERIC(5,2) NOT NULL DEFAULT 100 CHECK(traffic_percent BETWEEN 0 AND 100),
 created_by VARCHAR(200) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), stopped_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_sbase_ml_deployments_active ON sbase_ml_deployments(tenant_id,status);
CREATE TABLE IF NOT EXISTS sbase_ml_prediction_events (
 id BIGSERIAL PRIMARY KEY, tenant_id UUID NOT NULL, deployment_id UUID NOT NULL REFERENCES sbase_ml_deployments(id) ON DELETE CASCADE,
 latency_ms BIGINT NOT NULL, succeeded BOOLEAN NOT NULL, prediction_json JSONB, error_code VARCHAR(80),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sbase_ml_prediction_monitor ON sbase_ml_prediction_events(tenant_id,deployment_id,created_at DESC);
CREATE TABLE IF NOT EXISTS sbase_ml_batch_jobs (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, model_version_id UUID NOT NULL REFERENCES sbase_ml_model_versions(id),
 input_table VARCHAR(256) NOT NULL, output_table VARCHAR(256) NOT NULL,
 status VARCHAR(24) NOT NULL CHECK(status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED')),
 row_count BIGINT NOT NULL DEFAULT 0, error_message TEXT, submitted_by VARCHAR(200) NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ
);
-- Keep the current-value online table and append immutable event-time versions for point-in-time reads.
ALTER TABLE online_feature_values ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE online_feature_values DROP CONSTRAINT IF EXISTS online_feature_values_pkey;
ALTER TABLE online_feature_values ADD CONSTRAINT online_feature_values_pkey PRIMARY KEY(tenant_id,feature_set,entity_key);
CREATE INDEX IF NOT EXISTS idx_online_features_tenant_set ON online_feature_values(tenant_id,feature_set,event_time DESC);
CREATE TABLE IF NOT EXISTS offline_feature_values (
 id BIGSERIAL PRIMARY KEY, tenant_id UUID NOT NULL, feature_set VARCHAR(160) NOT NULL,
 entity_key VARCHAR(512) NOT NULL, feature_version INTEGER NOT NULL, feature_values JSONB NOT NULL,
 event_time TIMESTAMPTZ NOT NULL, ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 source_run_id UUID, UNIQUE(tenant_id,feature_set,entity_key,event_time,feature_version)
);
CREATE INDEX IF NOT EXISTS idx_offline_feature_asof ON offline_feature_values(tenant_id,feature_set,entity_key,event_time DESC);
CREATE TABLE IF NOT EXISTS sbase_feature_parity_checks (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, feature_set VARCHAR(160) NOT NULL,
 entity_key VARCHAR(512) NOT NULL, as_of TIMESTAMPTZ NOT NULL, is_match BOOLEAN NOT NULL,
 offline_hash VARCHAR(64), online_hash VARCHAR(64), differences_json JSONB NOT NULL DEFAULT '{}'::jsonb,
 checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_feature_parity_history ON sbase_feature_parity_checks(tenant_id,feature_set,checked_at DESC);
CREATE TABLE IF NOT EXISTS sbase_bi_reports (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, name VARCHAR(160) NOT NULL, description TEXT,
 definition_json JSONB NOT NULL, owner_subject VARCHAR(200) NOT NULL, version BIGINT NOT NULL DEFAULT 1,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS sbase_bi_report_grants (
 report_id UUID NOT NULL REFERENCES sbase_bi_reports(id) ON DELETE CASCADE, subject VARCHAR(200) NOT NULL,
 permission VARCHAR(16) NOT NULL CHECK(permission IN ('READ','QUERY','EDIT','ADMIN')),
 granted_by VARCHAR(200) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(report_id,subject)
);
CREATE TABLE IF NOT EXISTS sbase_bi_dashboards (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, name VARCHAR(160) NOT NULL, description TEXT,
 layout_json JSONB NOT NULL DEFAULT '{}'::jsonb, owner_subject VARCHAR(200) NOT NULL,
 is_shared BOOLEAN NOT NULL DEFAULT FALSE, version BIGINT NOT NULL DEFAULT 1,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS sbase_bi_widgets (
 id UUID PRIMARY KEY, dashboard_id UUID NOT NULL REFERENCES sbase_bi_dashboards(id) ON DELETE CASCADE,
 report_id UUID REFERENCES sbase_bi_reports(id) ON DELETE SET NULL, title VARCHAR(160) NOT NULL,
 widget_type VARCHAR(32) NOT NULL CHECK(widget_type IN ('TABLE','BAR','LINE','PIE','KPI','AREA')),
 position_json JSONB NOT NULL DEFAULT '{}'::jsonb, options_json JSONB NOT NULL DEFAULT '{}'::jsonb,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS sbase_bi_dashboard_grants (
 dashboard_id UUID NOT NULL REFERENCES sbase_bi_dashboards(id) ON DELETE CASCADE, subject VARCHAR(200) NOT NULL,
 permission VARCHAR(16) NOT NULL CHECK(permission IN ('READ','EDIT','ADMIN')),
 granted_by VARCHAR(200) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(dashboard_id,subject)
);
CREATE TABLE IF NOT EXISTS sbase_bi_schedules (
 id UUID PRIMARY KEY, tenant_id UUID NOT NULL, report_id UUID NOT NULL REFERENCES sbase_bi_reports(id) ON DELETE CASCADE,
 owner_subject VARCHAR(200) NOT NULL, cron_expression VARCHAR(120) NOT NULL, timezone VARCHAR(80) NOT NULL DEFAULT 'UTC',
 output_format VARCHAR(12) NOT NULL CHECK(output_format IN ('CSV','JSON')),
 delivery_target TEXT, enabled BOOLEAN NOT NULL DEFAULT TRUE, next_run_at TIMESTAMPTZ NOT NULL,
 lease_owner VARCHAR(80), lease_until TIMESTAMPTZ, last_run_at TIMESTAMPTZ, failure_count INTEGER NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sbase_bi_schedules_due ON sbase_bi_schedules(enabled,next_run_at,lease_until);
CREATE TABLE IF NOT EXISTS sbase_bi_schedule_runs (
 id UUID PRIMARY KEY, schedule_id UUID NOT NULL REFERENCES sbase_bi_schedules(id) ON DELETE CASCADE,
 status VARCHAR(24) NOT NULL, artifact_uri TEXT, row_count BIGINT NOT NULL DEFAULT 0,
 error_message TEXT, started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS sbase_bi_query_audit (
 id BIGSERIAL PRIMARY KEY, tenant_id UUID NOT NULL, subject VARCHAR(200) NOT NULL,
 query_hash VARCHAR(64) NOT NULL, elapsed_ms BIGINT NOT NULL, scanned_rows BIGINT NOT NULL DEFAULT 0,
 returned_rows INTEGER NOT NULL DEFAULT 0, estimated_cost NUMERIC(20,4) NOT NULL DEFAULT 0,
 cache_hit BOOLEAN NOT NULL DEFAULT FALSE, status VARCHAR(24) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sbase_bi_query_audit_tenant ON sbase_bi_query_audit(tenant_id,created_at DESC);

ALTER TABLE sbase_ml_batch_jobs ADD COLUMN IF NOT EXISTS result_json JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE sbase_ml_model_versions ADD COLUMN IF NOT EXISTS promoted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_sbase_ml_model_lineage ON sbase_ml_model_versions(tenant_id,run_id);
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES
 ('ml:read'),('ml:write'),('ml:deploy'),('ml:predict'),('bi:read'),('bi:write'),('bi:query')
) p(permission) WHERE r.name='ADMIN' ON CONFLICT DO NOTHING;

ALTER TABLE sbase_ml_prediction_events ADD COLUMN IF NOT EXISTS input_json JSONB NOT NULL DEFAULT '{}'::jsonb;
