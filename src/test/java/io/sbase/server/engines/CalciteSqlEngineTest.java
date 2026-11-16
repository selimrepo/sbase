package io.sbase.server.engines;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CalciteSqlEngineTest {
    private final CalciteSqlEngine engine = new CalciteSqlEngine();

    @Test
    void executesSqlAgainstWorkflowLocalJsonTables() {
        String tables = """
            {"orders":[
              {"id":1,"amount":5},
              {"id":2,"amount":15},
              {"id":3,"amount":25}
            ]}
            """;
        String result = engine.executeJson(
            "SELECT id, amount * 2 AS doubled FROM orders WHERE amount > 10 ORDER BY id",
            tables
        );
        assertTrue(result.contains("\"id\":2"));
        assertTrue(result.contains("\"doubled\":30"));
        assertTrue(result.contains("\"id\":3"));
        assertFalse(result.contains("\"id\":1"));
    }

    @Test
    void supportsSqlWithoutRegisteredTables() {
        String result = engine.executeJson("SELECT 6 * 7 AS answer", "{}");
        assertTrue(result.contains("\"answer\":42"));
    }

    @Test
    void rejectsUnsafeTableIdentifiers() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> engine.executeJson("SELECT 1", "{\"orders;drop\":[{\"id\":1}]}"));
        assertTrue(error.getMessage().contains("identifier"));
    }
}
