package io.sbase.server.automl;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RollingOriginPlanTest {
    @Test
    void createsExpandingNonOverlappingWalkForwardFolds() {
        List<RollingOriginPlan.Fold> folds = RollingOriginPlan.expandingWindow(80, 4);
        assertEquals(4, folds.size());
        long previousValidationEnd = 0;
        long previousTrainingEnd = 0;
        for (RollingOriginPlan.Fold fold : folds) {
            assertTrue(fold.trainingEndInclusive() > previousTrainingEnd);
            assertEquals(fold.trainingEndInclusive() + 1, fold.validationStartInclusive());
            assertTrue(fold.validationEndInclusive() >= fold.validationStartInclusive() + 1);
            if (previousValidationEnd > 0) assertEquals(previousValidationEnd + 1, fold.validationStartInclusive());
            previousTrainingEnd = fold.trainingEndInclusive();
            previousValidationEnd = fold.validationEndInclusive();
        }
        assertTrue(folds.get(3).validationEndInclusive() <= 80);
    }

    @Test
    void boundsFoldCountAndRejectsInsufficientRows() {
        assertEquals(5, RollingOriginPlan.expandingWindow(100, 99).size());
        assertThrows(IllegalArgumentException.class, () -> RollingOriginPlan.expandingWindow(7, 3));
    }
}
