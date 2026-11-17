package io.sbase.server.automl;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ModelDriftMonitorTest {
    @Test
    void detectsNumericMeanShiftAndMissingness() {
        Map<String, Object> numeric = new LinkedHashMap<>();
        numeric.put("numeric", true);
        numeric.put("mean", 10.0);
        numeric.put("stddev", 2.0);
        numeric.put("min", 5.0);
        numeric.put("max", 15.0);
        numeric.put("missingRate", 0.0);
        Map<String, Object> card = Map.of("featureProfile", Map.of("temperature", numeric));

        List<Map<String, Object>> rows = List.of(
                Map.of("temperature", 20.0),
                Map.of("temperature", 21.0),
                Collections.singletonMap("temperature", null));

        Map<String, Object> report = ModelDriftMonitor.evaluate(card, rows);
        assertEquals("HIGH", report.get("driftLevel"));
        @SuppressWarnings("unchecked")
        Map<String, Object> features = (Map<String, Object>) report.get("featureDrift");
        @SuppressWarnings("unchecked")
        Map<String, Object> temperature = (Map<String, Object>) features.get("temperature");
        assertEquals("HIGH", temperature.get("severity"));
        assertTrue(((Number) temperature.get("standardizedMeanShift")).doubleValue() > 2.0);
    }

    @Test
    void detectsPreviouslyUnseenCategories() {
        Map<String, Object> categorical = new LinkedHashMap<>();
        categorical.put("numeric", false);
        categorical.put("knownCategories", List.of("north", "south"));
        categorical.put("knownCategoriesTruncated", false);
        categorical.put("cardinality", 2);
        categorical.put("missingRate", 0.0);
        Map<String, Object> card = Map.of("featureProfile", Map.of("region", categorical));

        List<Map<String, Object>> rows = List.of(
                Map.of("region", "east"),
                Map.of("region", "east"),
                Map.of("region", "north"),
                Map.of("region", "south"),
                Map.of("region", "south"));

        Map<String, Object> report = ModelDriftMonitor.evaluate(card, rows);
        assertEquals("HIGH", report.get("driftLevel"));
        @SuppressWarnings("unchecked")
        Map<String, Object> features = (Map<String, Object>) report.get("featureDrift");
        @SuppressWarnings("unchecked")
        Map<String, Object> region = (Map<String, Object>) features.get("region");
        assertEquals(0.4, ((Number) region.get("unseenCategoryRate")).doubleValue(), 1.0e-9);
    }

    @Test
    void rejectsEmptyMonitoringBatches() {
        assertThrows(IllegalArgumentException.class,
                () -> ModelDriftMonitor.evaluate(Map.of("featureProfile", Map.of("x", Map.of())), List.of()));
    }
}
