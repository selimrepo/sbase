ALTER TABLE ingestion_dead_letters
    ADD COLUMN replay_started_at TIMESTAMPTZ,
    ADD COLUMN replay_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN last_replay_error TEXT;

CREATE INDEX idx_ingestion_dlq_replay_claim
    ON ingestion_dead_letters(replay_started_at)
    WHERE replayed_at IS NULL;
