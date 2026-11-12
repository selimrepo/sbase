-- Persist tenant ownership on governed BI schedule execution history.
ALTER TABLE sbase_bi_schedule_runs ADD COLUMN IF NOT EXISTS tenant_id UUID;
UPDATE sbase_bi_schedule_runs r
   SET tenant_id=s.tenant_id
  FROM sbase_bi_schedules s
 WHERE r.schedule_id=s.id AND r.tenant_id IS NULL;
ALTER TABLE sbase_bi_schedule_runs ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE sbase_bi_schedule_runs
    ADD CONSTRAINT fk_sbase_bi_schedule_runs_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id);
CREATE INDEX IF NOT EXISTS idx_sbase_bi_schedule_runs_tenant_history
    ON sbase_bi_schedule_runs(tenant_id,schedule_id,started_at DESC);

CREATE OR REPLACE FUNCTION sbase_derive_bi_schedule_run_tenant() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE schedule_tenant UUID;
BEGIN
    SELECT tenant_id INTO schedule_tenant
      FROM sbase_bi_schedules
     WHERE id=NEW.schedule_id;
    IF schedule_tenant IS NULL THEN
        RAISE EXCEPTION 'BI schedule % has no tenant owner', NEW.schedule_id;
    END IF;
    IF NEW.tenant_id IS NOT NULL AND NEW.tenant_id <> schedule_tenant THEN
        RAISE EXCEPTION 'BI schedule run tenant does not match schedule tenant';
    END IF;
    NEW.tenant_id := schedule_tenant;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_sbase_bi_schedule_run_tenant
BEFORE INSERT OR UPDATE OF schedule_id,tenant_id ON sbase_bi_schedule_runs
FOR EACH ROW EXECUTE FUNCTION sbase_derive_bi_schedule_run_tenant();
