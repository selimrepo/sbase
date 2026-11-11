package com.sbase.pipeline;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
class PipelineLeaseCoordinatorIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    void restartedNodeReclaimsExpiredWorkAndOldWorkerCannotPublishResult() {
        try (HikariDataSource ds = new HikariDataSource()) {
            ds.setJdbcUrl(postgres.getJdbcUrl());
            ds.setUsername(postgres.getUsername());
            ds.setPassword(postgres.getPassword());
            JdbcTemplate jdbc = new JdbcTemplate(ds);
            jdbc.execute("""
                CREATE TABLE pipeline_runs(
                  id UUID PRIMARY KEY,status VARCHAR(32) NOT NULL,started_at TIMESTAMPTZ,
                  finished_at TIMESTAMPTZ,attempt_count INT NOT NULL DEFAULT 0,
                  next_attempt_at TIMESTAMPTZ,lease_owner VARCHAR(160),lease_until TIMESTAMPTZ,
                  lease_heartbeat_at TIMESTAMPTZ,fencing_token BIGINT NOT NULL DEFAULT 0,
                  error_message TEXT,result_json JSONB,records_processed BIGINT NOT NULL DEFAULT 0,
                  max_attempts INT NOT NULL DEFAULT 5)
                """);
            UUID runId = UUID.randomUUID();
            jdbc.update("INSERT INTO pipeline_runs(id,status) VALUES(?,'QUEUED')", runId);

            // Separate coordinator instances represent independent application nodes.
            PipelineLeaseCoordinator nodeA = new PipelineLeaseCoordinator(jdbc, 10);
            PipelineLeaseCoordinator nodeB = new PipelineLeaseCoordinator(jdbc, 10);
            PipelineLeaseCoordinator.Lease first = nodeA.claim(runId, "node-a");
            assertNotNull(first);

            // Abrupt process loss: no heartbeat/shutdown hook; PostgreSQL lease expires.
            jdbc.update("UPDATE pipeline_runs SET lease_until=now()-interval '1 second' WHERE id=?", runId);
            PipelineLeaseCoordinator.Lease recovered = nodeB.claim(runId, "node-b");
            assertNotNull(recovered);
            assertTrue(recovered.fencingToken() > first.fencingToken());

            assertFalse(nodeA.complete(first, "node-a", "{}", 1),
                    "The dead/restarted worker must not publish a stale result");
            assertTrue(nodeB.complete(recovered, "node-b", null, 1));
            assertEquals("SUCCEEDED", jdbc.queryForObject(
                    "SELECT status FROM pipeline_runs WHERE id=?", String.class, runId));
            assertEquals(2, jdbc.queryForObject(
                    "SELECT attempt_count FROM pipeline_runs WHERE id=?", Integer.class, runId));
        }
    }
}
