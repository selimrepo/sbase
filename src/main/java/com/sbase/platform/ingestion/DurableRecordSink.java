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
                    INSERT INTO ingestion_records(id,connector_id,source_key,payload,created_at)
                    VALUES (?, ?, ?, ?::jsonb, now())
                    ON CONFLICT (connector_id, source_key) DO NOTHING
                    """, UUID.randomUUID(), connectorId, record.sourceKey(), json);
            }
            if (checkpointKey != null) {
                jdbc.update("""
                    INSERT INTO ingestion_checkpoints(connector_id,checkpoint_key,checkpoint_value,updated_at)
                    VALUES (?,?,?,now())
                    ON CONFLICT (connector_id,checkpoint_key) DO UPDATE
                    SET checkpoint_value=EXCLUDED.checkpoint_value,updated_at=now()
                    """, connectorId, checkpointKey, checkpointValue);
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
                INSERT INTO ingestion_checkpoints(connector_id,checkpoint_key,checkpoint_value,updated_at)
                VALUES (?,?,?,now())
                ON CONFLICT (connector_id,checkpoint_key) DO NOTHING
                """, connectorId, key, newValue) > 0;
        }));
    }

    public record Envelope(String sourceKey, Map<String,Object> payload) {}
}
