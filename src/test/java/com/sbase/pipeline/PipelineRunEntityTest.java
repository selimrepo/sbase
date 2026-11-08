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
}
