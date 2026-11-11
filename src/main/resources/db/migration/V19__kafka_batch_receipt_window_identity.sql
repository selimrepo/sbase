-- Distinguish replayed identical Kafka batches from a larger replay window
-- that starts at the same offset after a consumer crash/rebalance.
ALTER TABLE ingestion_batch_receipts
    DROP CONSTRAINT IF EXISTS ingestion_batch_receipts_pkey;

ALTER TABLE ingestion_batch_receipts
    ADD CONSTRAINT ingestion_batch_receipts_pkey
    PRIMARY KEY (tenant_id, connector_id, topic, partition_id, first_offset, next_offset);
