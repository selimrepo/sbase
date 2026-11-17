package io.sbase.server.automl;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ModelPromotionPolicyTest {
    @Test
    void driftTriggerUsesConfiguredSeverityFloor() {
        assertTrue(ModelPromotionPolicy.shouldTrigger("HIGH", "MEDIUM"));
        assertTrue(ModelPromotionPolicy.shouldTrigger("MEDIUM", "MEDIUM"));
        assertFalse(ModelPromotionPolicy.shouldTrigger("LOW", "MEDIUM"));
    }

    @Test
    void improvementRespectsMetricDirectionAndPromotionGate() {
        double rmseImprovement = ModelPromotionPolicy.improvement(8.0, 10.0, false);
        double f1Improvement = ModelPromotionPolicy.improvement(0.91, 0.87, true);
        assertEquals(2.0, rmseImprovement, 1.0e-9);
        assertEquals(0.04, f1Improvement, 1.0e-9);
        assertTrue(ModelPromotionPolicy.shouldPromote(rmseImprovement, 1.0));
        assertFalse(ModelPromotionPolicy.shouldPromote(-0.2, 0.0));
    }

    @Test
    void rejectsInvalidPolicyLevelsAndNonFiniteScores() {
        assertThrows(IllegalArgumentException.class, () -> ModelPromotionPolicy.shouldTrigger("UNKNOWN", "HIGH"));
        assertThrows(IllegalArgumentException.class, () -> ModelPromotionPolicy.improvement(Double.NaN, 1.0, true));
    }
}
