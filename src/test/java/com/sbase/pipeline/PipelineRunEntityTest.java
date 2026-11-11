package com.sbase.pipeline;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class PipelineRunEntityTest {
    @Test
    void recordsSuccessfulLifecycleAndSnapshot() {
        PipelineRunEntity run = new PipelineRunEntity(UUID.randomUUID(), 7, "{\"steps\":[]}");
        assertEquals("QUEUED", run.getStatus());
        assertEquals(7, run.getPipelineVersion());
        assertEquals("{\"steps\":[]}", run.getDefinitionSnapshot());
        run.start();
        assertEquals("RUNNING", run.getStatus());
        assertNotNull(run.getStartedAt());
        run.succeed("{\"recordsProcessed\":12}", 12);
        assertEquals("SUCCEEDED", run.getStatus());
        assertEquals(12, run.getRecordsProcessed());
        assertNotNull(run.getFinishedAt());
    }

    @Test
    void recordsFailureAndMessage() {
        PipelineRunEntity run = new PipelineRunEntity(UUID.randomUUID(), 1, "{}");
        run.start();
        run.fail("Doris unavailable");
        assertEquals("FAILED", run.getStatus());
        assertEquals("Doris unavailable", run.getErrorMessage());
        assertNotNull(run.getFinishedAt());
    }

    @Test
    void retriesWithPersistableBackoffStateUntilAttemptLimit() {
        PipelineRunEntity run = new PipelineRunEntity(UUID.randomUUID(), 1, "{}");
        run.setMaxAttempts(3);
        run.start();
        assertTrue(run.scheduleRetry("temporary Doris outage", java.time.Instant.now().plusSeconds(10)));
        assertEquals("RETRY_WAIT", run.getStatus());
        assertEquals(1, run.getAttemptCount());
        assertNotNull(run.getNextAttemptAt());
        run.start();
        assertTrue(run.scheduleRetry("temporary Doris outage", java.time.Instant.now().plusSeconds(20)));
        run.start();
        assertFalse(run.scheduleRetry("final failure", java.time.Instant.now().plusSeconds(40)));
        assertEquals("FAILED", run.getStatus());
        assertEquals(3, run.getAttemptCount());
        assertNull(run.getNextAttemptAt());
    }

    @Test
    void recordsSubmitterAndSupportsCancellation() {
        PipelineRunEntity run = new PipelineRunEntity(UUID.randomUUID(), 2, "{\"steps\":[]}", "analyst-42");
        assertEquals("analyst-42", run.getSubmittedBy());
        run.start();
        run.cancel();
        assertEquals("CANCELLED", run.getStatus());
        assertEquals("Cancelled by user", run.getErrorMessage());
        assertNotNull(run.getFinishedAt());
        assertNull(run.getLeaseUntil());
    }

}
