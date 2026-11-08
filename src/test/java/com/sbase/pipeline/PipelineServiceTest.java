package com.sbase.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import static org.junit.jupiter.api.Assertions.*;

class PipelineServiceTest {
    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void acceptsPipelineWithExecutableSteps() {
        assertDoesNotThrow(() -> validator.parseAndValidate("""
            {"steps":[
              {"id":"source","type":"doris_read","table":"analytics.events"},
              {"id":"projection","type":"select","dependsOn":["source"],"columns":["id"]}
            ]}
            """));
    }

    @Test
    void rejectsPipelineWithoutSteps() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate("{\"nodes\":[]}"));
    }
}
