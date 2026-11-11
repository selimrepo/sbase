-- Asynchronous ingestion queue and durable Kafka partition progress.
ALTER TABLE ingestion_runs
    ADD COLUMN IF NOT EXISTS request_payload JSONB,
    ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(160),
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS lease_heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_ingestion_queue_claim
    ON ingestion_runs(submitted_at, id)
    WHERE status IN ('QUEUED','RETRY_WAIT');

CREATE INDEX IF NOT EXISTS idx_ingestion_queue_expired
    ON ingestion_runs(lease_until)
    WHERE status='RUNNING';

CREATE TABLE IF NOT EXISTS ingestion_partition_offsets (
    tenant_id UUID NOT NULL,
    connector_id UUID NOT NULL REFERENCES ingestion_connectors(id) ON DELETE CASCADE,
    topic VARCHAR(249) NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    next_offset BIGINT NOT NULL CHECK (next_offset >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, connector_id, topic, partition_id)
);

CREATE TABLE IF NOT EXISTS ingestion_batch_receipts (
    tenant_id UUID NOT NULL,
    connector_id UUID NOT NULL REFERENCES ingestion_connectors(id) ON DELETE CASCADE,
    topic VARCHAR(249) NOT NULL,
    partition_id INTEGER NOT NULL,
    first_offset BIGINT NOT NULL,
    next_offset BIGINT NOT NULL,
    record_count INTEGER NOT NULL CHECK (record_count > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, connector_id, topic, partition_id, first_offset),
    CHECK (next_offset > first_offset)
);
