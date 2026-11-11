package com.sbase.resilience;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises PostgreSQL row-lock election, lease recovery and stale-worker fencing. */
@Testcontainers(disabledWithoutDocker = true)
class MultiNodeLeaseIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    void twoNodesNeverOwnSameLeaseAndExpiredOwnerCannotCommit() throws Exception {
        try (Connection setup = connection(); Statement s = setup.createStatement()) {
            s.execute("CREATE TABLE work(id INT PRIMARY KEY,status TEXT NOT NULL,owner TEXT,lease_until TIMESTAMPTZ,token BIGINT NOT NULL DEFAULT 0)");
            s.execute("INSERT INTO work(id,status) VALUES(1,'QUEUED'),(2,'QUEUED')");
        }
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (Connection nodeA = connection()) {
            nodeA.setAutoCommit(false);
            try (Statement lock = nodeA.createStatement()) {
                lock.executeQuery("SELECT id FROM work WHERE id=1 FOR UPDATE");
            }
            Future<Integer> nodeB = pool.submit(() -> {
                try (Connection c = connection(); Statement s = c.createStatement();
                     ResultSet rs = s.executeQuery("""
                         WITH candidate AS (
                           SELECT id FROM work WHERE status='QUEUED'
                           ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
                         )
                         UPDATE work w SET status='RUNNING',owner='node-b',token=token+1,
                           lease_until=now()+interval '1 second'
                         FROM candidate c WHERE w.id=c.id RETURNING w.id
                         """)) {
                    assertTrue(rs.next());
                    return rs.getInt(1);
                }
            });
            assertEquals(2, nodeB.get(10, TimeUnit.SECONDS));
            nodeA.rollback();
        } finally {
            pool.shutdownNow();
        }

        long oldToken;
        try (Connection c = connection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("""
                UPDATE work SET status='RUNNING',owner='node-a',token=token+1,
                  lease_until=now()-interval '1 second'
                WHERE id=1 RETURNING token
                """)) {
            assertTrue(rs.next()); oldToken = rs.getLong(1);
        }
        long newToken;
        try (Connection c = connection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("""
                UPDATE work SET owner='node-b',token=token+1,lease_until=now()+interval '30 seconds'
                WHERE id=1 AND lease_until<now() RETURNING token
                """)) {
            assertTrue(rs.next()); newToken = rs.getLong(1);
        }
        assertTrue(newToken > oldToken);
        try (Connection c = connection(); Statement s = c.createStatement()) {
            assertEquals(0, s.executeUpdate("UPDATE work SET status='SUCCEEDED' WHERE id=1 AND owner='node-a' AND token=" + oldToken));
            assertEquals(1, s.executeUpdate("UPDATE work SET status='SUCCEEDED' WHERE id=1 AND owner='node-b' AND token=" + newToken));
        }
    }

    private static Connection connection() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }
}
