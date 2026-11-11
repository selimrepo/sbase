-- Partition assignment fencing: a rebalance increments the token before a new owner can write.
ALTER TABLE ingestion_partition_offsets
    ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(160),
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_ingestion_partition_lease_expiry
    ON ingestion_partition_offsets(lease_until)
    WHERE lease_owner IS NOT NULL;
