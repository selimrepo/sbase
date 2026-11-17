package io.sbase.server.automl;

import java.util.ArrayList;
import java.util.List;

/** Expanding-window walk-forward folds over already time-ordered rows. */
public final class RollingOriginPlan {
    private RollingOriginPlan() {}

    public record Fold(int number, long trainingEndInclusive,
                       long validationStartInclusive, long validationEndInclusive) {}

    public static List<Fold> expandingWindow(int rowCount, int requestedFolds) {
        if (rowCount < 8) throw new IllegalArgumentException("At least eight rows are required for rolling-origin validation");
        int folds = Math.max(2, Math.min(requestedFolds, 5));
        long initialWindow = Math.max(2L, rowCount / 2L);
        long remaining = rowCount - initialWindow;
        long validationWindow = (long) Math.ceil((double) remaining / folds);
        List<Fold> result = new ArrayList<>();
        for (int index = 0; index < folds; index++) {
            long trainingEnd = initialWindow + index * validationWindow;
            long validationStart = trainingEnd + 1;
            long validationEnd = Math.min(rowCount, initialWindow + (index + 1L) * validationWindow);
            if (validationEnd - validationStart + 1 >= 2) {
                result.add(new Fold(index + 1, trainingEnd, validationStart, validationEnd));
            }
        }
        if (result.size() < 2)
            throw new IllegalArgumentException("At least two rolling-origin folds must contain two or more validation rows");
        return List.copyOf(result);
    }
}
