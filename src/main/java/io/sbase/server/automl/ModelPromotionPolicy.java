package io.sbase.server.automl;

/** Pure policy decisions shared by drift-triggered retraining and model promotion. */
public final class ModelPromotionPolicy {
    private ModelPromotionPolicy() {}

    public static boolean shouldTrigger(String driftLevel, String triggerLevel) {
        return severity(driftLevel) >= severity(triggerLevel);
    }

    public static double improvement(double candidateScore, double incumbentScore, boolean higherIsBetter) {
        if (!Double.isFinite(candidateScore) || !Double.isFinite(incumbentScore))
            throw new IllegalArgumentException("Model scores must be finite");
        return higherIsBetter ? candidateScore - incumbentScore : incumbentScore - candidateScore;
    }

    public static boolean shouldPromote(double improvement, double minimumImprovement) {
        if (!Double.isFinite(improvement) || !Double.isFinite(minimumImprovement) || minimumImprovement < 0.0)
            throw new IllegalArgumentException("Promotion improvement values must be finite and threshold non-negative");
        return improvement >= minimumImprovement && improvement > 0.0;
    }

    private static int severity(String value) {
        return switch (value == null ? "" : value.toUpperCase(java.util.Locale.ROOT)) {
            case "LOW" -> 0;
            case "MEDIUM" -> 1;
            case "HIGH" -> 2;
            default -> throw new IllegalArgumentException("Drift level must be LOW, MEDIUM or HIGH");
        };
    }
}
