-- Extend tenant ownership metadata to secondary platform resources.
-- Existing records are backfilled into the reserved legacy tenant; tenant-aware
-- APIs must scope all new access by the signed JWT tenant claim.
ALTER TABLE mining_jobs ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE mining_jobs ADD COLUMN IF NOT EXISTS owner_subject VARCHAR(200) NOT NULL DEFAULT 'system';

ALTER TABLE ingestion_connectors ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE ingestion_connectors ADD COLUMN IF NOT EXISTS owner_subject VARCHAR(200) NOT NULL DEFAULT 'system';
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS submitted_by VARCHAR(200) NOT NULL DEFAULT 'system';
ALTER TABLE ingestion_records ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE ingestion_checkpoints ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE ingestion_dead_letters ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);

UPDATE ingestion_runs r SET tenant_id=c.tenant_id,submitted_by=COALESCE(r.submitted_by,'system')
FROM ingestion_connectors c WHERE c.id=r.connector_id;
UPDATE ingestion_records r SET tenant_id=c.tenant_id FROM ingestion_connectors c WHERE c.id=r.connector_id;
UPDATE ingestion_checkpoints r SET tenant_id=c.tenant_id FROM ingestion_connectors c WHERE c.id=r.connector_id;
UPDATE ingestion_dead_letters r SET tenant_id=c.tenant_id FROM ingestion_connectors c WHERE c.id=r.connector_id;

ALTER TABLE experiments ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE experiments ADD COLUMN IF NOT EXISTS owner_subject VARCHAR(200) NOT NULL DEFAULT 'system';
UPDATE experiments e SET tenant_id=d.tenant_id FROM datasets d WHERE e.dataset_id=d.id;

ALTER TABLE registered_models ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE registered_models ADD COLUMN IF NOT EXISTS owner_subject VARCHAR(200) NOT NULL DEFAULT 'system';
ALTER TABLE model_versions ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE model_versions v SET tenant_id=m.tenant_id FROM registered_models m WHERE v.model_id=m.id;
ALTER TABLE model_artifacts ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE model_artifacts a SET tenant_id=v.tenant_id FROM model_versions v WHERE a.model_version_id=v.id;

ALTER TABLE feature_sets ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE feature_sets f SET tenant_id=d.tenant_id FROM datasets d WHERE f.dataset_id=d.id;
ALTER TABLE feature_materializations ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE feature_materializations m SET tenant_id=f.tenant_id FROM feature_sets f WHERE m.feature_set_id=f.id;
ALTER TABLE feature_lineage ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE feature_lineage l SET tenant_id=f.tenant_id FROM feature_sets f WHERE l.feature_set_id=f.id;
ALTER TABLE feature_access_grants ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE feature_access_grants g SET tenant_id=f.tenant_id FROM feature_sets f WHERE g.feature_set_id=f.id;
ALTER TABLE online_feature_values ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);

ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS owner_subject VARCHAR(200) NOT NULL DEFAULT 'system';
UPDATE scheduled_jobs s SET tenant_id=p.tenant_id FROM pipelines p WHERE s.pipeline_id=p.id;
ALTER TABLE schedule_runs ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE schedule_runs r SET tenant_id=s.tenant_id FROM scheduled_jobs s WHERE r.schedule_id=s.id;

ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE dashboards SET owner_subject=COALESCE(owner_subject,'system');
ALTER TABLE dashboard_widgets ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE dashboard_widgets w SET tenant_id=d.tenant_id FROM dashboards d WHERE w.dashboard_id=d.id;

ALTER TABLE bi_reports ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE bi_reports SET owner_subject=COALESCE(owner_subject,'system');
ALTER TABLE bi_report_shares ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE bi_report_shares s SET tenant_id=r.tenant_id FROM bi_reports r WHERE s.report_id=r.id;
ALTER TABLE bi_report_schedules ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE bi_report_schedules s SET tenant_id=r.tenant_id FROM bi_reports r WHERE s.report_id=r.id;
ALTER TABLE bi_report_schedule_runs ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE bi_report_schedule_runs r SET tenant_id=s.tenant_id FROM bi_report_schedules s WHERE r.schedule_id=s.id;

ALTER TABLE model_evaluations ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE model_evaluations e SET tenant_id=x.tenant_id FROM experiments x WHERE e.experiment_id=x.id;
ALTER TABLE pipeline_versions ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
UPDATE pipeline_versions v SET tenant_id=p.tenant_id FROM pipelines p WHERE v.pipeline_id=p.id;

CREATE INDEX IF NOT EXISTS idx_mining_jobs_tenant_submitted ON mining_jobs(tenant_id,submitted_at DESC);
DROP INDEX IF EXISTS uq_ingestion_runs_idempotency;
CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_runs_tenant_idempotency ON ingestion_runs(tenant_id,idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ingestion_connectors_tenant ON ingestion_connectors(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_tenant_started ON ingestion_runs(tenant_id,started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_records_tenant_created ON ingestion_records(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_experiments_tenant_created ON experiments(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_models_tenant ON registered_models(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feature_sets_tenant ON feature_sets(tenant_id,updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_tenant_due ON scheduled_jobs(tenant_id,enabled,next_run_at);
CREATE INDEX IF NOT EXISTS idx_dashboards_tenant ON dashboards(tenant_id,updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_bi_reports_tenant ON bi_reports(tenant_id,updated_at DESC);

-- Default administrative role receives the new operational controls.
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES
  ('tenants:admin'),('resources:admin'),('audit:read'),('mining:read'),('mining:write'),
  ('datasets:read'),('datasets:write')
) p(permission) WHERE r.name='ADMIN' ON CONFLICT DO NOTHING;
INSERT INTO sbase_role_permissions(role_id,permission)
SELECT r.id,p.permission FROM sbase_roles r CROSS JOIN (VALUES
  ('mining:read'),('mining:write'),('datasets:read'),('datasets:write')
) p(permission) WHERE r.name='DATA_ENGINEER' ON CONFLICT DO NOTHING;

-- Derive tenant ownership for child records created by legacy workers/services.
CREATE OR REPLACE FUNCTION sbase_derive_child_tenant() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME = 'pipeline_versions' THEN
    SELECT tenant_id INTO NEW.tenant_id FROM pipelines WHERE id=NEW.pipeline_id;
  ELSIF TG_TABLE_NAME = 'schedule_runs' THEN
    SELECT tenant_id INTO NEW.tenant_id FROM scheduled_jobs WHERE id=NEW.schedule_id;
  ELSIF TG_TABLE_NAME IN ('feature_materializations','feature_lineage','feature_access_grants') THEN
    SELECT tenant_id INTO NEW.tenant_id FROM feature_sets WHERE id=NEW.feature_set_id;
  ELSIF TG_TABLE_NAME = 'model_versions' THEN
    SELECT tenant_id INTO NEW.tenant_id FROM registered_models WHERE id=NEW.model_id;
  ELSIF TG_TABLE_NAME = 'model_artifacts' THEN
    SELECT tenant_id INTO NEW.tenant_id FROM model_versions WHERE id=NEW.model_version_id;
  ELSIF TG_TABLE_NAME = 'model_evaluations' THEN
    IF NEW.experiment_id IS NOT NULL THEN
      SELECT tenant_id INTO NEW.tenant_id FROM experiments WHERE id=NEW.experiment_id;
    END IF;
  ELSIF TG_TABLE_NAME = 'dashboard_widgets' THEN
    SELECT tenant_id INTO NEW.tenant_id FROM dashboards WHERE id=NEW.dashboard_id;
  ELSIF TG_TABLE_NAME = 'bi_report_shares' THEN
    SELECT tenant_id INTO NEW.tenant_id FROM bi_reports WHERE id=NEW.report_id;
  ELSIF TG_TABLE_NAME = 'bi_report_schedules' THEN
    SELECT tenant_id INTO NEW.tenant_id FROM bi_reports WHERE id=NEW.report_id;
  ELSIF TG_TABLE_NAME = 'bi_report_schedule_runs' THEN
    SELECT tenant_id INTO NEW.tenant_id FROM bi_report_schedules WHERE id=NEW.schedule_id;
  ELSIF TG_TABLE_NAME IN ('ingestion_runs','ingestion_records','ingestion_checkpoints','ingestion_dead_letters') THEN
    SELECT tenant_id INTO NEW.tenant_id FROM ingestion_connectors WHERE id=NEW.connector_id;
  END IF;
  IF NEW.tenant_id IS NULL THEN
    RAISE EXCEPTION 'Unable to derive tenant_id for %', TG_TABLE_NAME;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_pipeline_versions_tenant BEFORE INSERT OR UPDATE ON pipeline_versions
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_schedule_runs_tenant BEFORE INSERT OR UPDATE ON schedule_runs
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_feature_materializations_tenant BEFORE INSERT OR UPDATE ON feature_materializations
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_feature_lineage_tenant BEFORE INSERT OR UPDATE ON feature_lineage
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_feature_access_grants_tenant BEFORE INSERT OR UPDATE ON feature_access_grants
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_model_versions_tenant BEFORE INSERT OR UPDATE ON model_versions
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_model_artifacts_tenant BEFORE INSERT OR UPDATE ON model_artifacts
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_model_evaluations_tenant BEFORE INSERT OR UPDATE ON model_evaluations
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_dashboard_widgets_tenant BEFORE INSERT OR UPDATE ON dashboard_widgets
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_bi_report_shares_tenant BEFORE INSERT OR UPDATE ON bi_report_shares
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_bi_report_schedules_tenant BEFORE INSERT OR UPDATE ON bi_report_schedules
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_bi_report_schedule_runs_tenant BEFORE INSERT OR UPDATE ON bi_report_schedule_runs
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_ingestion_runs_tenant BEFORE INSERT OR UPDATE ON ingestion_runs
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_ingestion_records_tenant BEFORE INSERT OR UPDATE ON ingestion_records
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_ingestion_checkpoints_tenant BEFORE INSERT OR UPDATE ON ingestion_checkpoints
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
CREATE TRIGGER trg_ingestion_dead_letters_tenant BEFORE INSERT OR UPDATE ON ingestion_dead_letters
FOR EACH ROW EXECUTE FUNCTION sbase_derive_child_tenant();
