ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;
CREATE TABLE ingestion_records (
    id UUID PRIMARY KEY,
    connector_id UUID NOT NULL REFERENCES ingestion_connectors(id),
    source_key VARCHAR(600) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(connector_id, source_key)
);
CREATE INDEX idx_ingestion_records_connector_created ON ingestion_records(connector_id,created_at DESC);
CREATE TABLE ingestion_checkpoints (
    connector_id UUID NOT NULL REFERENCES ingestion_connectors(id) ON DELETE CASCADE,
    checkpoint_key VARCHAR(500) NOT NULL,
    checkpoint_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(connector_id,checkpoint_key)
);
CREATE TABLE ingestion_dead_letters (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ingestion_runs(id),
    connector_id UUID NOT NULL REFERENCES ingestion_connectors(id),
    connector_type VARCHAR(40) NOT NULL,
    source_ref TEXT,
    payload JSONB NOT NULL,
    error_message TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ
);
CREATE INDEX idx_ingestion_dlq_pending ON ingestion_dead_letters(created_at) WHERE replayed_at IS NULL;
