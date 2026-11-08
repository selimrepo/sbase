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

    @Test
    void acceptsTwoSourceJoinWithInnerAndOuterJoinModes() {
        assertDoesNotThrow(() -> validator.parseAndValidate("""
            {"steps":[
              {"id":"customers","type":"doris_read","table":"analytics.customers"},
              {"id":"orders","type":"doris_read","table":"analytics.orders"},
              {"id":"joined","type":"join","dependsOn":["customers","orders"],
               "leftKey":"id","rightKey":"customer_id","joinType":"left"},
              {"id":"projection","type":"select","dependsOn":["joined"],"columns":["id","customer_id"]}
            ]}
            """));
    }

    @Test
    void rejectsJoinWithoutExactlyTwoInputs() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate("""
            {"steps":[
              {"id":"customers","type":"doris_read","table":"analytics.customers"},
              {"id":"joined","type":"join","dependsOn":["customers"],
               "leftKey":"id","rightKey":"customer_id","joinType":"inner"}
            ]}
            """));
    }

    @Test
    void rejectsUnsupportedJoinMode() {
        assertThrows(ResponseStatusException.class, () -> validator.parseAndValidate("""
            {"steps":[
              {"id":"left","type":"doris_read","table":"analytics.left_table"},
              {"id":"right","type":"doris_read","table":"analytics.right_table"},
              {"id":"joined","type":"join","dependsOn":["left","right"],
               "leftKey":"id","rightKey":"id","joinType":"cross"}
            ]}
            """));
    }

}
