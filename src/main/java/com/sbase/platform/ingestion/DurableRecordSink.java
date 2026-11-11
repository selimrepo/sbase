package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class DurableRecordSink {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper mapper;

    public DurableRecordSink(JdbcTemplate jdbc, TransactionTemplate transactions, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.mapper = mapper;
    }

    public long write(UUID connectorId, String source, List<Envelope> records, String checkpointKey, String checkpointValue) {
        return transactions.execute(status -> {
            long inserted = 0;
            for (Envelope record : records) {
                String json;
                try { json = mapper.writeValueAsString(record.payload()); }
                catch (Exception e) { throw new IllegalArgumentException("Record is not JSON serializable", e); }
                inserted += jdbc.update("""
                    INSERT INTO ingestion_records(id,connector_id,tenant_id,source_key,payload,created_at)
                    SELECT ?,c.id,c.tenant_id,?,?::jsonb,now() FROM ingestion_connectors c WHERE c.id=?
                    ON CONFLICT (connector_id, source_key) DO NOTHING
                    """, UUID.randomUUID(), record.sourceKey(), json, connectorId);
            }
            if (checkpointKey != null) {
                jdbc.update("""
                    INSERT INTO ingestion_checkpoints(connector_id,tenant_id,checkpoint_key,checkpoint_value,updated_at)
                    SELECT id,tenant_id,?,?,now() FROM ingestion_connectors WHERE id=?
                    ON CONFLICT (connector_id,checkpoint_key) DO UPDATE
                    SET checkpoint_value=EXCLUDED.checkpoint_value,updated_at=now()
                    """, checkpointKey, checkpointValue, connectorId);
            }
            return inserted;
        });
    }

    public String checkpoint(UUID connectorId, String key) {
        return jdbc.query("SELECT checkpoint_value FROM ingestion_checkpoints WHERE connector_id=? AND checkpoint_key=?",
            rs -> rs.next() ? rs.getString(1) : null, connectorId, key);
    }

    public boolean compareAndSetCheckpoint(UUID connectorId, String key, String expectedValue, String newValue) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            if (expectedValue != null) {
                int updated = jdbc.update("""
                    UPDATE ingestion_checkpoints SET checkpoint_value=?,updated_at=now()
                    WHERE connector_id=? AND checkpoint_key=? AND checkpoint_value=?
                    """, newValue, connectorId, key, expectedValue);
                if (updated > 0) return true;
            }
            return jdbc.update("""
                INSERT INTO ingestion_checkpoints(connector_id,tenant_id,checkpoint_key,checkpoint_value,updated_at)
                SELECT id,tenant_id,?,?,now() FROM ingestion_connectors WHERE id=?
                ON CONFLICT (connector_id,checkpoint_key) DO NOTHING
                """, key, newValue, connectorId) > 0;
        }));
    }


    /**
     * Atomically persists Kafka records, batch receipts, and per-partition next offsets.
     * Broker commits happen only after this transaction returns. Replayed batches are
     * suppressed by the receipt primary key; record source keys remain a second guard.
     */
    public long writeKafkaBatch(UUID connectorId, String topic, List<Envelope> records) {
        if (records == null || records.isEmpty()) return 0;
        return transactions.execute(status -> {
            UUID tenantId = jdbc.query("SELECT tenant_id FROM ingestion_connectors WHERE id=?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, connectorId);
            if (tenantId == null) throw new IllegalArgumentException("Unknown ingestion connector");
            Map<Integer, List<Envelope>> partitions = new java.util.TreeMap<>();
            for (Envelope record : records) {
                Object p = record.payload().get("partition");
                Object o = record.payload().get("offset");
                if (!(p instanceof Number) || !(o instanceof Number))
                    throw new IllegalArgumentException("Kafka envelope must contain numeric partition and offset");
                partitions.computeIfAbsent(((Number)p).intValue(), ignored -> new java.util.ArrayList<>()).add(record);
            }
            java.util.Set<String> accepted = new java.util.HashSet<>();
            long inserted = 0;
            for (var entry : partitions.entrySet()) {
                int partition = entry.getKey();
                List<Envelope> batch = entry.getValue();
                long first = batch.stream().mapToLong(e -> ((Number)e.payload().get("offset")).longValue()).min().orElseThrow();
                long next = batch.stream().mapToLong(e -> ((Number)e.payload().get("offset")).longValue()).max().orElseThrow() + 1;
                int receipt = jdbc.update("""
                    INSERT INTO ingestion_batch_receipts(tenant_id,connector_id,topic,partition_id,
                        first_offset,next_offset,record_count)
                    VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING
                    """, tenantId, connectorId, topic, partition, first, next, batch.size());
                if (receipt == 1) {
                    batch.forEach(e -> accepted.add(e.sourceKey()));
                    jdbc.update("""
                        INSERT INTO ingestion_partition_offsets(tenant_id,connector_id,topic,partition_id,next_offset,updated_at)
                        VALUES(?,?,?,?,?,now())
                        ON CONFLICT(tenant_id,connector_id,topic,partition_id) DO UPDATE
                          SET next_offset=GREATEST(ingestion_partition_offsets.next_offset,EXCLUDED.next_offset),
                              updated_at=now()
                        """, tenantId, connectorId, topic, partition, next);
                }
            }
            for (Envelope record : records) {
                if (!accepted.contains(record.sourceKey())) continue;
                String json;
                try { json = mapper.writeValueAsString(record.payload()); }
                catch (Exception e) { throw new IllegalArgumentException("Record is not JSON serializable", e); }
                inserted += jdbc.update("""
                    INSERT INTO ingestion_records(id,connector_id,tenant_id,source_key,payload,created_at)
                    VALUES(?,?,?, ?,?::jsonb,now())
                    ON CONFLICT(connector_id,source_key) DO NOTHING
                    """, UUID.randomUUID(), connectorId, tenantId, record.sourceKey(), json);
            }
            return inserted;
        });
    }

    public record Envelope(String sourceKey, Map<String,Object> payload) {}
}
