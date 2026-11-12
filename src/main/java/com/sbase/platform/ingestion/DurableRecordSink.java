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
            UUID tenantId=jdbc.query("SELECT tenant_id FROM ingestion_connectors WHERE id=?",
                rs->rs.next()?rs.getObject(1,UUID.class):null,connectorId);
            if(tenantId==null) throw new IllegalArgumentException("Unknown ingestion connector");
            long inserted = 0;
            for (Envelope record : records) {
                requireTenantRecord(tenantId,record);
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

    /** Claims a partition on assignment and returns a monotonically increasing fencing token. */
    public long claimKafkaPartition(UUID connectorId, String topic, int partition, String ownerId) {
        return transactions.execute(status -> jdbc.query("""
            INSERT INTO ingestion_partition_offsets(tenant_id,connector_id,topic,partition_id,
                next_offset,fencing_token,lease_owner,lease_until,updated_at)
            SELECT tenant_id,id,?,?,0,1,?,now()+interval '2 minutes',now()
              FROM ingestion_connectors WHERE id=?
            ON CONFLICT(tenant_id,connector_id,topic,partition_id) DO UPDATE
               SET fencing_token=ingestion_partition_offsets.fencing_token+1,
                   lease_owner=EXCLUDED.lease_owner,lease_until=now()+interval '2 minutes',updated_at=now()
            RETURNING fencing_token
            """, rs -> {
                if (!rs.next()) throw new IllegalArgumentException("Unknown Kafka connector");
                return rs.getLong(1);
            }, topic, partition, ownerId, connectorId));
    }

    public boolean renewKafkaPartition(UUID connectorId, String topic, int partition,
                                       String ownerId, long fencingToken) {
        return jdbc.update("""
            UPDATE ingestion_partition_offsets
               SET lease_until=now()+interval '2 minutes',updated_at=now()
             WHERE connector_id=? AND topic=? AND partition_id=? AND lease_owner=?
               AND fencing_token=? AND lease_until>now()
            """, connectorId, topic, partition, ownerId, fencingToken) == 1;
    }

    /** Revocation invalidates the current token before Kafka may assign the partition elsewhere. */
    public void releaseKafkaPartition(UUID connectorId, String topic, int partition,
                                      String ownerId, long fencingToken) {
        transactions.executeWithoutResult(status -> jdbc.update("""
            UPDATE ingestion_partition_offsets
               SET fencing_token=fencing_token+1,lease_owner=NULL,lease_until=NULL,updated_at=now()
             WHERE connector_id=? AND topic=? AND partition_id=? AND lease_owner=? AND fencing_token=?
            """, connectorId, topic, partition, ownerId, fencingToken));
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
    public long writeKafkaBatch(UUID connectorId, String topic, List<Envelope> records,
                                String ownerId, Map<Integer, Long> fencingTokens) {
        if (records == null || records.isEmpty()) return 0;
        if (ownerId == null || fencingTokens == null) throw new IllegalArgumentException("Kafka partition ownership is required");
        return transactions.execute(status -> {
            UUID tenantId = jdbc.query("SELECT tenant_id FROM ingestion_connectors WHERE id=?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, connectorId);
            if (tenantId == null) throw new IllegalArgumentException("Unknown ingestion connector");
            for(Envelope record:records) requireTenantRecord(tenantId,record);
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
                Long fencingToken = fencingTokens.get(partition);
                Long activeToken = jdbc.query("""
                    SELECT fencing_token FROM ingestion_partition_offsets
                     WHERE tenant_id=? AND connector_id=? AND topic=? AND partition_id=?
                       AND lease_owner=? AND fencing_token=? AND lease_until>now()
                     FOR UPDATE
                    """, rs -> rs.next() ? rs.getLong(1) : null,
                    tenantId, connectorId, topic, partition, ownerId, fencingToken);
                if (fencingToken == null || activeToken == null || !activeToken.equals(fencingToken)) {
                    throw new IllegalStateException("Kafka partition ownership was revoked; refusing stale offset progression");
                }
                long first = batch.stream().mapToLong(e -> ((Number)e.payload().get("offset")).longValue()).min().orElseThrow();
                long next = batch.stream().mapToLong(e -> ((Number)e.payload().get("offset")).longValue()).max().orElseThrow() + 1;
                int receipt = jdbc.update("""
                    INSERT INTO ingestion_batch_receipts(tenant_id,connector_id,topic,partition_id,
                        first_offset,next_offset,record_count)
                    VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING
                    """, tenantId, connectorId, topic, partition, first, next, batch.size());
                if (receipt == 1) {
                    batch.forEach(e -> accepted.add(e.sourceKey()));
                    int advanced = jdbc.update("""
                        UPDATE ingestion_partition_offsets
                           SET next_offset=GREATEST(next_offset,?),updated_at=now()
                         WHERE tenant_id=? AND connector_id=? AND topic=? AND partition_id=?
                           AND lease_owner=? AND fencing_token=? AND lease_until>now()
                        """, next, tenantId, connectorId, topic, partition, ownerId, fencingToken);
                    if (advanced != 1) throw new IllegalStateException("Kafka partition fencing token changed before offset advancement");
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

    private static void requireTenantRecord(UUID tenantId,Envelope record) {
        Object owner=record==null || record.payload()==null?null:record.payload().get("tenant_id");
        if(tenantId==null || owner==null || !tenantId.toString().equals(owner.toString()))
            throw new org.springframework.security.access.AccessDeniedException(
                "Ingestion record is missing tenant_id or belongs to another tenant");
    }

    public record Envelope(String sourceKey, Map<String,Object> payload) {}
}
