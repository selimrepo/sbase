package com.sbase.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.*;

class PipelineDefinitionValidatorTest {
    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void acceptsLinearPipeline() {
        assertDoesNotThrow(() -> validator.parseAndValidate("""
            {"steps":[
              {"id":"source","type":"doris_read","table":"analytics.events"},
              {"id":"filtered","type":"filter","dependsOn":["source"],"column":"age","operator":"gte","value":18},
              {"id":"projection","type":"select","dependsOn":["filtered"],"columns":["id","age"]},
              {"id":"persist","type":"doris_write","dependsOn":["projection"],"table":"analytics.filtered_events","mode":"append","idempotencyColumns":["id"]}
            ]}
            """));
    }

    @Test
    void rejectsMalformedJson() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate("{"));
    }

    @Test
    void rejectsUnknownStepType() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate(
                "{\"steps\":[{\"id\":\"x\",\"type\":\"execute_sql\"}]}"));
    }

    @Test
    void rejectsUnsafeTableIdentifier() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate(
                "{\"steps\":[{\"id\":\"x\",\"type\":\"doris_read\",\"table\":\"analytics.events;drop table x\"}]}"));
    }

    @Test
    void rejectsUnknownDependency() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate(
                "{\"steps\":[{\"id\":\"x\",\"type\":\"doris_read\",\"table\":\"analytics.events\",\"dependsOn\":[\"missing\"]}]}"));
    }

    @Test
    void rejectsDependencyCycle() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate("""
            {"steps":[
              {"id":"a","type":"doris_read","table":"analytics.a","dependsOn":["b"]},
              {"id":"b","type":"doris_read","table":"analytics.b","dependsOn":["a"]}
            ]}
            """));
    }

    @Test
    void rejectsMultipleDependenciesUntilJoinOperatorExists() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate("""
            {"steps":[
              {"id":"a","type":"doris_read","table":"analytics.a"},
              {"id":"b","type":"doris_read","table":"analytics.b"},
              {"id":"c","type":"select","dependsOn":["a","b"],"columns":["id"]}
            ]}
            """));
    }

    @Test
    void rejectsEmptyPipeline() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate("{\"steps\":[]}"));
    }

    @Test
    void rejectsUnsafeColumnName() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate(
                "{\"steps\":[{\"id\":\"x\",\"type\":\"select\",\"columns\":[\"id;drop\"]}]}"));
    }

    @Test
    void rejectsInvalidWriteMode() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate(
                "{\"steps\":[{\"id\":\"x\",\"type\":\"doris_write\",\"table\":\"analytics.out\",\"mode\":\"ignore\"}]}"));
    }

    @Test
    void rejectsDuplicateStepIds() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate("""
            {"steps":[
              {"id":"same","type":"doris_read","table":"analytics.a"},
              {"id":"same","type":"doris_read","table":"analytics.b"}
            ]}
            """));
    }
}
