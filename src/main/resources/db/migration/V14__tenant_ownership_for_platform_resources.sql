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
CREATE INDEX IF NOT EXISTS idx_ingestion_connectors_tenant ON ingestion_connectors(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_tenant_started ON ingestion_runs(tenant_id,started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_records_tenant_created ON ingestion_records(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_experiments_tenant_created ON experiments(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_models_tenant ON registered_models(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feature_sets_tenant ON feature_sets(tenant_id,updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_tenant_due ON scheduled_jobs(tenant_id,enabled,next_run_at);
CREATE INDEX IF NOT EXISTS idx_dashboards_tenant ON dashboards(tenant_id,updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_bi_reports_tenant ON bi_reports(tenant_id,updated_at DESC);
