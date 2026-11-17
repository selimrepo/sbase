package io.sbase.server.automl;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class AutoMlMonitoringServiceTest {
    @Test
    void stableNumericDistributionIsNotFlagged() {
        List<Map<String,Object>> reference = rows(100, i -> i);
        List<Map<String,Object>> current = rows(100, i -> i);
        Map<String,Object> report = AutoMlMonitoringService.compare(reference, current,
                List.of("x"), 0.20, 0.05, 0.10);
        assertEquals("STABLE", report.get("status"));
        assertTrue(((List<?>) report.get("driftedFeatures")).isEmpty());
    }

    @Test
    void shiftedNumericDistributionIsFlaggedByPsiAndKs() {
        List<Map<String,Object>> reference = rows(100, i -> i);
        List<Map<String,Object>> current = rows(100, i -> i + 1000);
        Map<String,Object> report = AutoMlMonitoringService.compare(reference, current,
                List.of("x"), 0.20, 0.05, 0.10);
        assertEquals("DRIFT_DETECTED", report.get("status"));
        Map<?,?> feature = (Map<?,?>) ((List<?>) report.get("features")).get(0);
        assertTrue(((Number) feature.get("psi")).doubleValue() > 0.20);
        assertTrue(((Number) feature.get("ksStatistic")).doubleValue() > 0.9);
    }

    @Test
    void categoricalAndMissingnessShiftAreDetected() {
        List<Map<String,Object>> reference = new ArrayList<>();
        List<Map<String,Object>> current = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            reference.add(Map.of("segment", "A"));
            current.add(i < 80 ? Map.of("segment", "B") : Collections.singletonMap("segment", null));
        }
        Map<String,Object> report = AutoMlMonitoringService.compare(reference, current,
                List.of("segment"), 0.20, 0.05, 0.10);
        assertEquals("DRIFT_DETECTED", report.get("status"));
        Map<?,?> feature = (Map<?,?>) ((List<?>) report.get("features")).get(0);
        assertTrue(((Number) feature.get("psi")).doubleValue() > 0.20);
        assertTrue((Boolean) feature.get("driftDetected"));
    }

    private static List<Map<String,Object>> rows(int count, java.util.function.IntFunction<Integer> value) {
        List<Map<String,Object>> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) rows.add(Map.of("x", value.apply(i)));
        return rows;
    }
}
