package io.sbase.server.jobs;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class JobRepositoryPostgresTest {
    static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("sbase")
            .withUsername("sbase")
            .withPassword("sbase-test");
    static HikariDataSource dataSource;
    static JobRepository repository;

    @BeforeAll
    static void startDatabase() {
        POSTGRES.start();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        dataSource = new HikariDataSource(config);
        repository = new JobRepository(dataSource, 1);
        repository.migrate();
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) dataSource.close();
        POSTGRES.stop();
    }

    @Test
    void expiredLeaseCanBeRecoveredAndOldWorkerIsFenced() throws Exception {
        var workflow = repository.createWorkflow("recovery-test",
            "{\"nodes\":[{\"id\":\"sum\",\"operator\":\"mahout-vector-sum\",\"config\":{\"values\":[1,2]}}]}");
        UUID jobId = repository.submit(workflow._2(), "recovery-" + UUID.randomUUID(), 3);
        var firstClaim = repository.claim(100);
        JobLease first = null;
        var firstIterator = firstClaim.iterator();
        while (firstIterator.hasNext()) { var candidate = firstIterator.next(); if (candidate.id().equals(jobId)) first = candidate; }
        assertNotNull(first);

        Thread.sleep(1300);
        var secondClaim = repository.claim(100);
        JobLease second = null;
        var secondIterator = secondClaim.iterator();
        while (secondIterator.hasNext()) { var candidate = secondIterator.next(); if (candidate.id().equals(jobId)) second = candidate; }
        assertNotNull(second);

        assertNotEquals(first.leaseOwner(), second.leaseOwner(), "lease fencing token must rotate");
        assertFalse(repository.complete(first, "{}"), "expired worker must not commit stale output");
        assertTrue(repository.complete(second, "{\"ok\":true}"));
    }

    @Test
    void idempotencyKeyReturnsSameJob() {
        var workflow = repository.createWorkflow("idempotency-test", "{\"nodes\":[]}");
        String key = "same-key-" + UUID.randomUUID();
        UUID first = repository.submit(workflow._2(), key, 2);
        UUID second = repository.submit(workflow._2(), key, 2);
        assertEquals(first, second);
    }
}
