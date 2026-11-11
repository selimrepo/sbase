package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
class KafkaPartitionFencingIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    void revokedConsumerCannotPersistBatchOrAdvanceOffsetAfterNewOwnerClaimsPartition() {
        try (HikariDataSource ds = new HikariDataSource()) {
            ds.setJdbcUrl(postgres.getJdbcUrl());
            ds.setUsername(postgres.getUsername());
            ds.setPassword(postgres.getPassword());
            JdbcTemplate jdbc = new JdbcTemplate(ds);
            jdbc.execute("CREATE TABLE ingestion_connectors(id UUID PRIMARY KEY,tenant_id UUID NOT NULL)");
            jdbc.execute("""
                CREATE TABLE ingestion_partition_offsets(
                  tenant_id UUID NOT NULL,connector_id UUID NOT NULL,topic VARCHAR(249) NOT NULL,
                  partition_id INT NOT NULL,next_offset BIGINT NOT NULL,fencing_token BIGINT NOT NULL DEFAULT 0,
                  lease_owner VARCHAR(160),lease_until TIMESTAMPTZ,updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY(tenant_id,connector_id,topic,partition_id))
                """);
            jdbc.execute("""
                CREATE TABLE ingestion_batch_receipts(
                  tenant_id UUID NOT NULL,connector_id UUID NOT NULL,topic VARCHAR(249) NOT NULL,
                  partition_id INT NOT NULL,first_offset BIGINT NOT NULL,next_offset BIGINT NOT NULL,
                  record_count INT NOT NULL,PRIMARY KEY(tenant_id,connector_id,topic,partition_id,first_offset))
                """);
            jdbc.execute("""
                CREATE TABLE ingestion_records(
                  id UUID PRIMARY KEY,connector_id UUID NOT NULL,tenant_id UUID NOT NULL,
                  source_key TEXT NOT NULL,payload JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL,
                  UNIQUE(connector_id,source_key))
                """);
            UUID tenant = UUID.randomUUID(), connector = UUID.randomUUID();
            jdbc.update("INSERT INTO ingestion_connectors(id,tenant_id) VALUES(?,?)", connector, tenant);
            DurableRecordSink sink = new DurableRecordSink(jdbc,
                    new TransactionTemplate(new DataSourceTransactionManager(ds)), new ObjectMapper());

            long oldToken = sink.claimKafkaPartition(connector, "events", 0, "consumer-old");
            sink.releaseKafkaPartition(connector, "events", 0, "consumer-old", oldToken);
            long newToken = sink.claimKafkaPartition(connector, "events", 0, "consumer-new");
            assertTrue(newToken > oldToken);

            var record = new DurableRecordSink.Envelope("events:0:10",
                    Map.of("topic", "events", "partition", 0, "offset", 10, "value", "payload"));
            assertThrows(IllegalStateException.class, () -> sink.writeKafkaBatch(
                    connector, "events", List.of(record), "consumer-old", Map.of(0, oldToken)));
            assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM ingestion_batch_receipts", Integer.class));
            assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM ingestion_records", Integer.class));

            assertEquals(1, sink.writeKafkaBatch(connector, "events", List.of(record),
                    "consumer-new", Map.of(0, newToken)));
            assertEquals(11L, jdbc.queryForObject("SELECT next_offset FROM ingestion_partition_offsets WHERE connector_id=?",
                    Long.class, connector));
        }
    }
}
