package io.sbase.server.automl;

import java.time.Instant;
import java.util.*;

/** Lightweight production drift diagnostics against the training profile stored in a model card. */
public final class ModelDriftMonitor {
    private ModelDriftMonitor() {}

    @SuppressWarnings("unchecked")
    public static Map<String, Object> evaluate(Map<String, Object> modelCard, List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty() || rows.size() > 50000)
            throw new IllegalArgumentException("Monitoring requires 1..50000 rows");
        Object rawProfile = modelCard.get("featureProfile");
        if (!(rawProfile instanceof Map<?, ?> profile) || profile.isEmpty())
            throw new IllegalStateException("Model has no training feature profile");

        Map<String, Object> featureReports = new LinkedHashMap<>();
        int overall = 0; // 0=LOW, 1=MEDIUM, 2=HIGH
        for (Map.Entry<?, ?> entry : profile.entrySet()) {
            String feature = String.valueOf(entry.getKey());
            if (!(entry.getValue() instanceof Map<?, ?> rawStats))
                continue;
            Map<String, Object> stats = (Map<String, Object>) rawStats;
            boolean numeric = Boolean.TRUE.equals(stats.get("numeric"));
            long missing = rows.stream().filter(row -> row.get(feature) == null).count();
            double missingRate = (double) missing / rows.size();
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("rows", rows.size());
            report.put("missingRate", missingRate);
            int severity = missingRate >= 0.20 ? 2 : missingRate >= 0.05 ? 1 : 0;

            if (numeric) {
                List<Double> values = new ArrayList<>();
                for (Map<String, Object> row : rows) {
                    Object value = row.get(feature);
                    if (value instanceof Number number && Double.isFinite(number.doubleValue()))
                        values.add(number.doubleValue());
                }
                if (!values.isEmpty()) {
                    double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                    double baselineMean = number(stats.get("mean"));
                    double baselineStd = Math.max(Math.abs(number(stats.get("stddev"))), 1.0e-9);
                    double meanShift = Math.abs(mean - baselineMean) / baselineStd;
                    double min = number(stats.get("min"));
                    double max = number(stats.get("max"));
                    long outside = values.stream().filter(value -> value < min || value > max).count();
                    double outOfRangeRate = (double) outside / values.size();
                    report.put("numeric", true);
                    report.put("mean", mean);
                    report.put("trainingMean", baselineMean);
                    report.put("standardizedMeanShift", meanShift);
                    report.put("outOfTrainingRangeRate", outOfRangeRate);
                    if (meanShift >= 2.0 || outOfRangeRate >= 0.20) severity = Math.max(severity, 2);
                    else if (meanShift >= 1.0 || outOfRangeRate >= 0.05) severity = Math.max(severity, 1);
                } else {
                    report.put("numeric", true);
                    report.put("status", "NO_VALID_NUMERIC_VALUES");
                    severity = 2;
                }
            } else {
                report.put("numeric", false);
                Object truncated = stats.get("knownCategoriesTruncated");
                if (!Boolean.TRUE.equals(truncated) && stats.get("knownCategories") instanceof List<?> known) {
                    Set<String> categories = new HashSet<>();
                    known.forEach(value -> categories.add(String.valueOf(value)));
                    long observed = rows.stream().filter(row -> row.get(feature) != null).count();
                    long unseen = rows.stream().filter(row -> row.get(feature) != null
                            && !categories.contains(String.valueOf(row.get(feature)))).count();
                    double unseenRate = observed == 0 ? 0.0 : (double) unseen / observed;
                    report.put("unseenCategoryRate", unseenRate);
                    if (unseenRate >= 0.20) severity = Math.max(severity, 2);
                    else if (unseenRate >= 0.05) severity = Math.max(severity, 1);
                } else {
                    report.put("unseenCategoryRate", null);
                    report.put("categoryDriftStatus", "BASELINE_CATEGORY_SET_TRUNCATED");
                }
            }
            report.put("severity", severityName(severity));
            overall = Math.max(overall, severity);
            featureReports.put(feature, report);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("evaluatedRows", rows.size());
        result.put("driftLevel", severityName(overall));
        result.put("featureDrift", featureReports);
        result.put("thresholds", Map.of(
                "mediumStandardizedMeanShift", 1.0,
                "highStandardizedMeanShift", 2.0,
                "mediumRate", 0.05,
                "highRate", 0.20));
        result.put("createdAt", Instant.now().toString());
        result.put("interpretation", "Drift thresholds are operational heuristics; review data semantics before retraining.");
        return result;
    }

    private static double number(Object value) {
        return value instanceof Number number ? number.doubleValue() : 0.0;
    }

    private static String severityName(int value) {
        return switch (value) {
            case 2 -> "HIGH";
            case 1 -> "MEDIUM";
            default -> "LOW";
        };
    }
}
