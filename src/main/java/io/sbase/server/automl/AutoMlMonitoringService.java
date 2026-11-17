package io.sbase.server.automl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.util.*;

/** Statistical feature-drift monitoring using PSI and the two-sample Kolmogorov-Smirnov statistic. */
public final class AutoMlMonitoringService {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final String jdbcUrl = env("SBASE_POSTGRES_URL", "jdbc:postgresql://localhost:5432/sbase");
    private final String user = env("SBASE_POSTGRES_USER", "sbase");
    private final String password = env("SBASE_POSTGRES_PASSWORD", "");

    public Map<String, Object> analyze(Map<String, Object> request) throws Exception {
        List<Map<String, Object>> reference = rows(request.get("reference"), "reference");
        List<Map<String, Object>> current = rows(request.get("current"), "current");
        List<String> features = ((List<?>) request.getOrDefault("features", List.of())).stream()
                .map(String::valueOf).toList();
        if (features.isEmpty()) throw new IllegalArgumentException("features must contain at least one column");
        double psiThreshold = number(request.get("psiThreshold"), 0.20);
        double ksAlpha = number(request.get("ksAlpha"), 0.05);
        double missingThreshold = number(request.get("missingRateDeltaThreshold"), 0.10);
        if (psiThreshold <= 0 || ksAlpha <= 0 || ksAlpha >= 1 || missingThreshold < 0 || missingThreshold > 1) {
            throw new IllegalArgumentException("Invalid drift thresholds");
        }
        Map<String, Object> report = compare(reference, current, features, psiThreshold, ksAlpha, missingThreshold);
        UUID runId = UUID.randomUUID();
        UUID modelId = request.get("modelId") == null ? null : UUID.fromString(String.valueOf(request.get("modelId")));
        persist(runId, modelId, reference.size(), current.size(), psiThreshold, String.valueOf(report.get("status")), report);
        report.put("runId", runId.toString());
        report.put("modelId", modelId == null ? null : modelId.toString());
        return report;
    }

    public List<Map<String, Object>> runs(UUID modelId, int limit) throws Exception {
        String sql = "SELECT id,model_id,reference_rows,current_rows,status,report::text,created_at " +
                "FROM model_monitoring_run " + (modelId == null ? "" : "WHERE model_id=? ") +
                "ORDER BY created_at DESC LIMIT ?";
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            if (modelId != null) statement.setObject(index++, modelId);
            statement.setInt(index, Math.max(1, Math.min(limit, 200)));
            try (ResultSet result = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (result.next()) {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("id", result.getObject("id").toString());
                    item.put("modelId", result.getObject("model_id") == null ? null : result.getObject("model_id").toString());
                    item.put("referenceRows", result.getInt("reference_rows"));
                    item.put("currentRows", result.getInt("current_rows"));
                    item.put("status", result.getString("status"));
                    item.put("report", JSON.readValue(result.getString("report"), new TypeReference<Map<String, Object>>() {}));
                    item.put("createdAt", result.getTimestamp("created_at").toInstant().toString());
                    rows.add(item);
                }
                return rows;
            }
        }
    }

    public static Map<String, Object> compare(List<Map<String, Object>> reference,
                                               List<Map<String, Object>> current,
                                               List<String> features,
                                               double psiThreshold,
                                               double ksAlpha,
                                               double missingThreshold) {
        if (reference == null || current == null || reference.isEmpty() || current.isEmpty()) {
            throw new IllegalArgumentException("Both reference and current datasets must be non-empty");
        }
        if (reference.size() > 100_000 || current.size() > 100_000) {
            throw new IllegalArgumentException("Each drift dataset is limited to 100000 rows");
        }
        List<Map<String, Object>> featureReports = new ArrayList<>();
        List<String> drifted = new ArrayList<>();
        for (String feature : features) {
            List<Object> refValues = values(reference, feature);
            List<Object> curValues = values(current, feature);
            boolean numeric = refValues.stream().filter(Objects::nonNull).allMatch(Number.class::isInstance)
                    && curValues.stream().filter(Objects::nonNull).allMatch(Number.class::isInstance)
                    && refValues.stream().anyMatch(Number.class::isInstance)
                    && curValues.stream().anyMatch(Number.class::isInstance);
            double refMissing = missingRate(refValues);
            double curMissing = missingRate(curValues);
            double psi;
            Double ks = null;
            Double pValue = null;
            if (numeric) {
                List<Double> ref = numbers(refValues);
                List<Double> cur = numbers(curValues);
                psi = numericPsi(ref, cur);
                ks = ksStatistic(ref, cur);
                pValue = ksPValue(ks, ref.size(), cur.size());
            } else {
                psi = categoricalPsi(refValues, curValues);
            }
            double missingDelta = Math.abs(curMissing - refMissing);
            boolean detected = psi >= psiThreshold || (pValue != null && pValue < ksAlpha)
                    || missingDelta >= missingThreshold;
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("feature", feature);
            item.put("type", numeric ? "NUMERIC" : "CATEGORICAL");
            item.put("psi", psi);
            item.put("ksStatistic", ks);
            item.put("ksPValueApprox", pValue);
            item.put("referenceMissingRate", refMissing);
            item.put("currentMissingRate", curMissing);
            item.put("missingRateDelta", missingDelta);
            item.put("driftDetected", detected);
            featureReports.add(item);
            if (detected) drifted.add(feature);
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("status", drifted.isEmpty() ? "STABLE" : "DRIFT_DETECTED");
        report.put("referenceRows", reference.size());
        report.put("currentRows", current.size());
        report.put("psiThreshold", psiThreshold);
        report.put("ksAlpha", ksAlpha);
        report.put("missingRateDeltaThreshold", missingThreshold);
        report.put("driftedFeatures", drifted);
        report.put("features", featureReports);
        report.put("statisticalNotes", List.of(
                "PSI uses reference-derived quantile bins for numeric features and category frequencies for categorical features.",
                "KS p-values use the asymptotic two-sample Kolmogorov-Smirnov approximation.",
                "Drift is a distribution-change signal, not proof of model degradation or causality."
        ));
        return report;
    }

    private void persist(UUID id, UUID modelId, int referenceRows, int currentRows, double threshold,
                         String status, Map<String, Object> report) throws Exception {
        String sql = "INSERT INTO model_monitoring_run(id,model_id,reference_rows,current_rows,psi_threshold,status,report) " +
                "VALUES (?,?,?,?,?,?,?::jsonb)";
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, id);
            statement.setObject(2, modelId);
            statement.setInt(3, referenceRows);
            statement.setInt(4, currentRows);
            statement.setDouble(5, threshold);
            statement.setString(6, status);
            statement.setString(7, JSON.writeValueAsString(report));
            statement.executeUpdate();
        }
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    private static List<Map<String, Object>> rows(Object value, String name) {
        if (!(value instanceof List<?> list) || list.isEmpty() || list.size() > 100_000) {
            throw new IllegalArgumentException(name + " must contain between 1 and 100000 rows");
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object row : list) {
            if (!(row instanceof Map<?, ?> map)) throw new IllegalArgumentException(name + " rows must be objects");
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, item) -> copy.put(String.valueOf(key), item));
            result.add(copy);
        }
        return result;
    }

    private static List<Object> values(List<Map<String, Object>> rows, String feature) {
        List<Object> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) result.add(row.get(feature));
        return result;
    }

    private static List<Double> numbers(List<Object> values) {
        List<Double> result = new ArrayList<>();
        for (Object value : values) if (value instanceof Number number && Double.isFinite(number.doubleValue())) result.add(number.doubleValue());
        return result;
    }

    private static double missingRate(List<Object> values) {
        long missing = values.stream().filter(v -> v == null || (v instanceof String s && s.isBlank())).count();
        return (double) missing / values.size();
    }

    private static double numericPsi(List<Double> reference, List<Double> current) {
        if (reference.isEmpty() || current.isEmpty()) return 0.0;
        List<Double> sorted = new ArrayList<>(reference);
        Collections.sort(sorted);
        List<Double> cuts = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            double cut = sorted.get(Math.min(sorted.size() - 1, (int) Math.floor((double) i * sorted.size() / 10)));
            if (cuts.isEmpty() || cut > cuts.get(cuts.size() - 1)) cuts.add(cut);
        }
        int bins = cuts.size() + 1;
        int[] expected = new int[bins];
        int[] actual = new int[bins];
        for (double value : reference) expected[bin(cuts, value)]++;
        for (double value : current) actual[bin(cuts, value)]++;
        return psi(expected, reference.size(), actual, current.size());
    }

    private static int bin(List<Double> cuts, double value) {
        int index = 0;
        while (index < cuts.size() && value > cuts.get(index)) index++;
        return index;
    }

    private static double categoricalPsi(List<Object> reference, List<Object> current) {
        Map<String, Integer> expected = frequencies(reference);
        Map<String, Integer> actual = frequencies(current);
        Set<String> categories = new HashSet<>(expected.keySet());
        categories.addAll(actual.keySet());
        double result = 0.0;
        for (String category : categories) {
            double e = (double) expected.getOrDefault(category, 0) / reference.size();
            double a = (double) actual.getOrDefault(category, 0) / current.size();
            result += (a - e) * Math.log((a + 1.0e-6) / (e + 1.0e-6));
        }
        return result;
    }

    private static Map<String, Integer> frequencies(List<Object> values) {
        Map<String, Integer> result = new HashMap<>();
        for (Object value : values) {
            String key = value == null || (value instanceof String s && s.isBlank()) ? "__MISSING__" : String.valueOf(value);
            result.merge(key, 1, Integer::sum);
        }
        return result;
    }

    private static double psi(int[] expected, int expectedTotal, int[] actual, int actualTotal) {
        double result = 0.0;
        for (int i = 0; i < expected.length; i++) {
            double e = (double) expected[i] / expectedTotal;
            double a = (double) actual[i] / actualTotal;
            result += (a - e) * Math.log((a + 1.0e-6) / (e + 1.0e-6));
        }
        return result;
    }

    private static double ksStatistic(List<Double> left, List<Double> right) {
        if (left.isEmpty() || right.isEmpty()) return 0.0;
        List<Double> a = new ArrayList<>(left), b = new ArrayList<>(right);
        Collections.sort(a); Collections.sort(b);
        int i = 0, j = 0;
        double maximum = 0.0;
        while (i < a.size() || j < b.size()) {
            double nextA = i < a.size() ? a.get(i) : Double.POSITIVE_INFINITY;
            double nextB = j < b.size() ? b.get(j) : Double.POSITIVE_INFINITY;
            double value = Math.min(nextA, nextB);
            while (i < a.size() && a.get(i) <= value) i++;
            while (j < b.size() && b.get(j) <= value) j++;
            maximum = Math.max(maximum, Math.abs((double) i / a.size() - (double) j / b.size()));
        }
        return maximum;
    }

    private static double ksPValue(double statistic, int n1, int n2) {
        if (n1 == 0 || n2 == 0) return 1.0;
        double effective = Math.sqrt((double) n1 * n2 / (n1 + n2));
        double lambda = (effective + 0.12 + 0.11 / effective) * statistic;
        double sum = 0.0;
        for (int k = 1; k <= 100; k++) {
            double term = Math.exp(-2.0 * k * k * lambda * lambda);
            sum += (k % 2 == 1 ? 1.0 : -1.0) * term;
            if (term < 1.0e-12) break;
        }
        return Math.max(0.0, Math.min(1.0, 2.0 * sum));
    }

    private static double number(Object value, double fallback) {
        return value instanceof Number number ? number.doubleValue() : fallback;
    }

    private static String env(String key, String fallback) {
        return System.getenv().getOrDefault(key, fallback);
    }
}
