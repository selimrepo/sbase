package io.sbase.server.jobs;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class JobRepositoryPostgresTest {
    static HikariDataSource dataSource;
    static JobRepository repository;

    @BeforeAll
    static void startDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(System.getenv().getOrDefault("SBASE_TEST_POSTGRES_URL", "jdbc:postgresql://localhost:5432/sbase"));
        config.setUsername(System.getenv().getOrDefault("SBASE_TEST_POSTGRES_USER", "sbase"));
        config.setPassword(System.getenv().getOrDefault("SBASE_TEST_POSTGRES_PASSWORD", "sbase-test"));
        dataSource = new HikariDataSource(config);
        repository = new JobRepository(dataSource, 1);
        repository.migrate();
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) dataSource.close();
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
    void expiredFinalAttemptIsMarkedFailedInsteadOfRequeuedForever() throws Exception {
        var workflow = repository.createWorkflow("final-attempt-test", "{\"nodes\":[]}");
        UUID jobId = repository.submit(workflow._2(), "final-" + UUID.randomUUID(), 1);
        var claims = repository.claim(100);
        JobLease lease = null;
        var iterator = claims.iterator();
        while (iterator.hasNext()) { var candidate = iterator.next(); if (candidate.id().equals(jobId)) lease = candidate; }
        assertNotNull(lease);
        Thread.sleep(1300);
        repository.claim(100);
        var statusOption = repository.status(jobId);
        assertTrue(statusOption.isDefined());
        var status = statusOption.get();
        assertEquals("FAILED", status._1());
    }

    @Test
    void workflowVersionsCanBeCreatedAndActivated() {
        var workflow = repository.createWorkflow("version-test", "{\"nodes\":[]}");
        int second = repository.createWorkflowVersion(workflow._2(), "{\"nodes\":[]}", true);
        assertEquals(2, second);
        assertTrue(repository.activateWorkflowVersion(workflow._2(), 1));
        assertEquals(2, repository.workflowVersions(workflow._2()).size());
        assertFalse(repository.activateWorkflowVersion(workflow._2(), 99));
    }

    @Test
    void dueScheduleCreatesDurableJobAndCanBePaused() {
        var workflow = repository.createWorkflow("schedule-test", "{\"nodes\":[]}");
        UUID scheduleId = repository.createSchedule(workflow._2(), 10, 2);
        assertEquals(1, repository.dispatchDueSchedules(10));
        assertEquals(1, repository.schedules(scala.Option.apply(workflow._2())).size());
        assertTrue(repository.setScheduleEnabled(scheduleId, false));
        assertFalse(repository.setScheduleEnabled(UUID.randomUUID(), false));
    }

    @Test
    void idempotencyKeyReturnsSameJob() {
        var workflow = repository.createWorkflow("idempotency-test", "{\"nodes\":[]}");
        String key = "same-key-" + UUID.randomUUID();
        UUID first = repository.submit(workflow._2(), key, 2);
        UUID second = repository.submit(workflow._2(), key, 2);
        assertEquals(first, second);
    }

    @Test
    void publishedDeploymentPinsVersionAndEnforcesConcurrencyAndAudit() {
        var workflow = repository.createWorkflow("deployment-test", "{\"nodes\":[]}");
        UUID deploymentId = repository.publishDeployment(workflow._2(), "scientific-pipeline", 1, 1, 3);
        assertNotNull(deploymentId);
        var deployment = repository.deployment("scientific-pipeline");
        assertTrue(deployment.isDefined());
        assertNotNull(deployment.get());

        var first = repository.invokeDeployment("scientific-pipeline", "external-request-1", 3);
        assertEquals(1, first._2());
        assertThrows(IllegalStateException.class,
            () -> repository.invokeDeployment("scientific-pipeline", "external-request-2", 3));

        var retry = repository.invokeDeployment("scientific-pipeline", "external-request-1", 3);
        assertEquals(first._1(), retry._1(), "idempotent retries must return the original run");
        assertTrue(repository.setDeploymentEnabled("scientific-pipeline", false));
        assertThrows(IllegalStateException.class,
            () -> repository.invokeDeployment("scientific-pipeline", "external-request-3", 3));
        assertTrue(repository.setDeploymentEnabled("scientific-pipeline", true));

        var audit = repository.workflowAudit(workflow._2(), 50);
        boolean published = false, invoked = false, paused = false, resumed = false;
        var auditIterator = audit.iterator();
        while (auditIterator.hasNext()) {
            var event = auditIterator.next();
            String eventType = String.valueOf(event.apply("eventType"));
            published |= "DEPLOYMENT_PUBLISHED".equals(eventType);
            invoked |= "DEPLOYMENT_INVOKED".equals(eventType);
            paused |= "DEPLOYMENT_PAUSED".equals(eventType);
            resumed |= "DEPLOYMENT_RESUMED".equals(eventType);
        }
        assertTrue(published); assertTrue(invoked); assertTrue(paused); assertTrue(resumed);
    }

    @Test
    void deploymentCanBeUpdatedToAnotherImmutableWorkflowVersion() {
        var workflow = repository.createWorkflow("deployment-version-test", "{\"nodes\":[]}");
        UUID deploymentId = repository.publishDeployment(workflow._2(), "versioned-service", 1, 2, 4);
        int secondVersion = repository.createWorkflowVersion(workflow._2(), "{\"nodes\":[{\"id\":\"x\",\"operator\":\"dataset-values\",\"config\":{\"values\":[1]}}]}", false);
        assertEquals(2, secondVersion);
        assertEquals(deploymentId, repository.publishDeployment(workflow._2(), "versioned-service", 2, 2, 4));
        var run = repository.invokeDeployment("versioned-service", "version-2-request", 4);
        assertEquals(2, run._2());
    }

}
