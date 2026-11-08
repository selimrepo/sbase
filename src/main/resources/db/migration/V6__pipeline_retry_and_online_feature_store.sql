ALTER TABLE pipeline_runs
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 4,
    ADD COLUMN next_attempt_at TIMESTAMPTZ,
    ADD COLUMN idempotency_key VARCHAR(160),
    ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

CREATE INDEX idx_pipeline_runs_retry_due
    ON pipeline_runs(status, next_attempt_at)
    WHERE status IN ('QUEUED', 'RETRY_WAIT');

CREATE TABLE online_feature_values (
    feature_set VARCHAR(160) NOT NULL,
    entity_key VARCHAR(512) NOT NULL,
    feature_version INTEGER NOT NULL DEFAULT 1,
    feature_values JSONB NOT NULL,
    event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(feature_set, entity_key)
);
CREATE INDEX idx_online_feature_values_event_time
    ON online_feature_values(feature_set, event_time DESC);
