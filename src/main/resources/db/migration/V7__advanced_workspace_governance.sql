CREATE TABLE pipeline_versions (
 id UUID PRIMARY KEY, pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
 version INTEGER NOT NULL, name VARCHAR(160) NOT NULL, description TEXT, definition_json TEXT NOT NULL,
 created_by VARCHAR(200) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(pipeline_id,version));
CREATE INDEX idx_pipeline_versions_history ON pipeline_versions(pipeline_id,version DESC);
CREATE OR REPLACE FUNCTION sbase_capture_pipeline_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.version IS DISTINCT FROM NEW.version OR OLD.definition_json IS DISTINCT FROM NEW.definition_json THEN
  INSERT INTO pipeline_versions(id,pipeline_id,version,name,description,definition_json,created_by)
  VALUES(gen_random_uuid(),OLD.id,OLD.version,OLD.name,OLD.description,OLD.definition_json,
         COALESCE(NULLIF(current_setting('sbase.subject',true),''),'system'))
  ON CONFLICT(pipeline_id,version) DO NOTHING;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER trg_pipeline_version_history AFTER UPDATE ON pipelines
 FOR EACH ROW EXECUTE FUNCTION sbase_capture_pipeline_version();

CREATE TABLE bi_reports (
 id UUID PRIMARY KEY,name VARCHAR(160) NOT NULL,description TEXT,owner_subject VARCHAR(200) NOT NULL,
 definition_json TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX idx_bi_reports_owner ON bi_reports(owner_subject,updated_at DESC);

CREATE TABLE model_evaluations (
 id UUID PRIMARY KEY,experiment_id UUID REFERENCES experiments(id) ON DELETE SET NULL,name VARCHAR(160) NOT NULL,
 task_type VARCHAR(24) NOT NULL CHECK(task_type IN ('REGRESSION','CLASSIFICATION')),
 metrics_json TEXT NOT NULL,sample_count INTEGER NOT NULL CHECK(sample_count>0),created_by VARCHAR(200) NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX idx_model_evaluations_experiment ON model_evaluations(experiment_id,created_at DESC);

CREATE TABLE feature_lineage (
 id UUID PRIMARY KEY,feature_set_id UUID NOT NULL REFERENCES feature_sets(id) ON DELETE CASCADE,
 source_dataset_id UUID REFERENCES datasets(id) ON DELETE SET NULL,source_columns_json TEXT NOT NULL DEFAULT '[]',
 parent_feature_ids_json TEXT NOT NULL DEFAULT '[]',transformation_json TEXT NOT NULL,created_by VARCHAR(200) NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX idx_feature_lineage_feature ON feature_lineage(feature_set_id,created_at DESC);
CREATE TABLE feature_access_grants (
 feature_set_id UUID NOT NULL REFERENCES feature_sets(id) ON DELETE CASCADE,subject VARCHAR(200) NOT NULL,
 permission VARCHAR(16) NOT NULL CHECK(permission IN ('READ','WRITE','ADMIN')),granted_by VARCHAR(200) NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),PRIMARY KEY(feature_set_id,subject));
ALTER TABLE feature_sets ADD COLUMN IF NOT EXISTS lifecycle_state VARCHAR(24) NOT NULL DEFAULT 'ACTIVE';

ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
ALTER TABLE dashboard_widgets ADD COLUMN IF NOT EXISTS data_source VARCHAR(24) NOT NULL DEFAULT 'BI_REPORT';
ALTER TABLE dashboard_widgets ADD COLUMN IF NOT EXISTS refresh_seconds INTEGER NOT NULL DEFAULT 0;
ALTER TABLE dashboard_widgets ADD CONSTRAINT dashboard_widgets_refresh_seconds_check CHECK(refresh_seconds BETWEEN 0 AND 86400);

ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS timezone VARCHAR(80) NOT NULL DEFAULT 'UTC';
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS calendar_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS depends_on UUID REFERENCES scheduled_jobs(id) ON DELETE SET NULL;
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS failure_policy VARCHAR(24) NOT NULL DEFAULT 'RETRY';
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS misfire_policy VARCHAR(24) NOT NULL DEFAULT 'RUN_ONCE';
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ;
CREATE TABLE schedule_runs (
 id UUID PRIMARY KEY,schedule_id UUID NOT NULL REFERENCES scheduled_jobs(id) ON DELETE CASCADE,
 pipeline_run_id UUID REFERENCES pipeline_runs(id) ON DELETE SET NULL,status VARCHAR(24) NOT NULL,
 scheduled_for TIMESTAMPTZ NOT NULL,started_at TIMESTAMPTZ,finished_at TIMESTAMPTZ,
 attempt INTEGER NOT NULL DEFAULT 1,error_message TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX idx_schedule_runs_history ON schedule_runs(schedule_id,created_at DESC);
