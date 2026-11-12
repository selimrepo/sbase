-- Enforce tenant ownership relationships at the PostgreSQL constraint layer.
-- Fail migration rather than silently repair or expose already-corrupt cross-tenant rows.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM ingestion_runs r JOIN ingestion_connectors c ON c.id=r.connector_id
        WHERE r.tenant_id<>c.tenant_id
    ) THEN RAISE EXCEPTION 'Cross-tenant ingestion_runs.connector_id ownership found; repair before V23'; END IF;
    IF EXISTS (
        SELECT 1 FROM ingestion_records r JOIN ingestion_connectors c ON c.id=r.connector_id
        WHERE r.tenant_id<>c.tenant_id
    ) THEN RAISE EXCEPTION 'Cross-tenant ingestion_records ownership found; repair before V23'; END IF;
    IF EXISTS (
        SELECT 1 FROM ingestion_checkpoints r JOIN ingestion_connectors c ON c.id=r.connector_id
        WHERE r.tenant_id<>c.tenant_id
    ) THEN RAISE EXCEPTION 'Cross-tenant ingestion_checkpoints ownership found; repair before V23'; END IF;
    IF EXISTS (
        SELECT 1 FROM ingestion_dead_letters d
        JOIN ingestion_connectors c ON c.id=d.connector_id
        JOIN ingestion_runs r ON r.id=d.run_id
        WHERE d.tenant_id<>c.tenant_id OR d.tenant_id<>r.tenant_id
    ) THEN RAISE EXCEPTION 'Cross-tenant ingestion_dead_letters ownership found; repair before V23'; END IF;
    IF EXISTS (
        SELECT 1 FROM ingestion_partition_offsets p JOIN ingestion_connectors c ON c.id=p.connector_id
        WHERE p.tenant_id<>c.tenant_id
    ) THEN RAISE EXCEPTION 'Cross-tenant Kafka partition ownership found; repair before V23'; END IF;
    IF EXISTS (
        SELECT 1 FROM ingestion_batch_receipts p JOIN ingestion_connectors c ON c.id=p.connector_id
        WHERE p.tenant_id<>c.tenant_id
    ) THEN RAISE EXCEPTION 'Cross-tenant Kafka batch receipt found; repair before V23'; END IF;
    IF EXISTS (
        SELECT 1 FROM pipeline_runs r JOIN pipelines p ON p.id=r.pipeline_id
        WHERE r.tenant_id<>p.tenant_id
    ) THEN RAISE EXCEPTION 'Cross-tenant pipeline run ownership found; repair before V23'; END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_connectors_id_tenant ON ingestion_connectors(id,tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_runs_id_tenant ON ingestion_runs(id,tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pipelines_id_tenant ON pipelines(id,tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_runs_id_tenant ON pipeline_runs(id,tenant_id);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_ingestion_runs_connector_tenant') THEN
        ALTER TABLE ingestion_runs ADD CONSTRAINT fk_ingestion_runs_connector_tenant
            FOREIGN KEY(connector_id,tenant_id) REFERENCES ingestion_connectors(id,tenant_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_ingestion_records_connector_tenant') THEN
        ALTER TABLE ingestion_records ADD CONSTRAINT fk_ingestion_records_connector_tenant
            FOREIGN KEY(connector_id,tenant_id) REFERENCES ingestion_connectors(id,tenant_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_ingestion_checkpoints_connector_tenant') THEN
        ALTER TABLE ingestion_checkpoints ADD CONSTRAINT fk_ingestion_checkpoints_connector_tenant
            FOREIGN KEY(connector_id,tenant_id) REFERENCES ingestion_connectors(id,tenant_id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_ingestion_dead_letters_connector_tenant') THEN
        ALTER TABLE ingestion_dead_letters ADD CONSTRAINT fk_ingestion_dead_letters_connector_tenant
            FOREIGN KEY(connector_id,tenant_id) REFERENCES ingestion_connectors(id,tenant_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_ingestion_dead_letters_run_tenant') THEN
        ALTER TABLE ingestion_dead_letters ADD CONSTRAINT fk_ingestion_dead_letters_run_tenant
            FOREIGN KEY(run_id,tenant_id) REFERENCES ingestion_runs(id,tenant_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_ingestion_partition_connector_tenant') THEN
        ALTER TABLE ingestion_partition_offsets ADD CONSTRAINT fk_ingestion_partition_connector_tenant
            FOREIGN KEY(connector_id,tenant_id) REFERENCES ingestion_connectors(id,tenant_id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_ingestion_receipts_connector_tenant') THEN
        ALTER TABLE ingestion_batch_receipts ADD CONSTRAINT fk_ingestion_receipts_connector_tenant
            FOREIGN KEY(connector_id,tenant_id) REFERENCES ingestion_connectors(id,tenant_id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_pipeline_runs_pipeline_tenant') THEN
        ALTER TABLE pipeline_runs ADD CONSTRAINT fk_pipeline_runs_pipeline_tenant
            FOREIGN KEY(pipeline_id,tenant_id) REFERENCES pipelines(id,tenant_id);
    END IF;
END $$;

-- Feature keys are tenant-local; the former global primary key could collide
-- across tenants and force unrelated tenants to share an entity namespace.
ALTER TABLE online_feature_values DROP CONSTRAINT IF EXISTS online_feature_values_pkey;
ALTER TABLE online_feature_values ADD CONSTRAINT online_feature_values_pkey
    PRIMARY KEY(tenant_id,feature_set,entity_key);
CREATE INDEX IF NOT EXISTS idx_online_feature_values_tenant_event
    ON online_feature_values(tenant_id,feature_set,event_time DESC);

-- The online feature upsert must match the new tenant-local key.
