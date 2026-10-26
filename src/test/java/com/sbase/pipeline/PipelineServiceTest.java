package com.sbase.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import static org.junit.jupiter.api.Assertions.*;

class PipelineServiceTest {
    @Test
    void acceptsPipelineWithStepsArray() {
        PipelineService service = new PipelineService(null, new ObjectMapper());
        assertDoesNotThrow(() -> {
            var method = PipelineService.class.getDeclaredMethod("validateDefinition", String.class);
            method.setAccessible(true);
            method.invoke(service, "{\"steps\":[{\"type\":\"read-doris\"},{\"type\":\"select\"}]}");
        });
    }
    @Test
    void rejectsPipelineWithoutSteps() {
        PipelineService service = new PipelineService(null, new ObjectMapper());
        assertThrows(Exception.class, () -> {
            var method = PipelineService.class.getDeclaredMethod("validateDefinition", String.class);
            method.setAccessible(true);
            method.invoke(service, "{\"nodes\":[]}");
        });
    }
}