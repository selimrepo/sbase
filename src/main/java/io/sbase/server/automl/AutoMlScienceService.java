package io.sbase.server.automl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Dataset diagnostics for scientific AutoML: schema, missingness, distributions and quality warnings. */
public final class AutoMlScienceService {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> NUMERIC = Set.of("byte", "short", "integer", "long", "float", "double", "decimal");
    private final SparkSession spark;

    public AutoMlScienceService(SparkSession spark) {
        this.spark = Objects.requireNonNull(spark);
    }

    public Map<String, Object> profile(Map<String, Object> request) {
        List<?> input = (List<?>) request.getOrDefault("data", List.of());
        if (input.isEmpty() || input.size() > 100_000) {
            throw new IllegalArgumentException("data must contain between 1 and 100000 rows");
        }

        List<String> jsonRows = new ArrayList<>(input.size());
        for (Object row : input) {
            if (!(row instanceof Map<?, ?>)) throw new IllegalArgumentException("Every data row must be an object");
            jsonRows.add(JSON.valueToTree(row).toString());
        }
        Dataset<Row> data = spark.read().json(spark.createDataset(jsonRows, Encoders.STRING()));
        List<String> requested = ((List<?>) request.getOrDefault("features", List.of())).stream()
                .map(String::valueOf).toList();
        String label = request.get("label") == null ? null : String.valueOf(request.get("label"));
        List<String> features = requested.isEmpty()
                ? Arrays.stream(data.columns()).filter(c -> !Objects.equals(c, label)).toList()
                : requested;
        if (features.isEmpty()) throw new IllegalArgumentException("No feature columns found");
        Set<String> columns = new HashSet<>(Arrays.asList(data.columns()));
        if (!columns.containsAll(features)) throw new IllegalArgumentException("Unknown feature column");

        long rowCount = data.count();
        long distinctRows = data.dropDuplicates().count();
        List<Map<String, Object>> profiles = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        for (String feature : features) {
            String type = data.schema().apply(feature).dataType().typeName().toLowerCase(Locale.ROOT);
            org.apache.spark.sql.Column value = org.apache.spark.sql.functions.col(feature);
            org.apache.spark.sql.Column missing = value.isNull().or(
                    org.apache.spark.sql.functions.trim(value.cast("string")).equalTo(""));
            long missingCount = data.filter(missing).count();
            long distinctCount = data.select(value).distinct().count();
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("feature", feature);
            item.put("dataType", type);
            item.put("missingCount", missingCount);
            item.put("missingRate", ratio(missingCount, rowCount));
            item.put("distinctCount", distinctCount);
            item.put("constant", distinctCount <= 1);
            if (NUMERIC.contains(type)) {
                Row stats = data.agg(
                        org.apache.spark.sql.functions.min(value),
                        org.apache.spark.sql.functions.max(value),
                        org.apache.spark.sql.functions.avg(value),
                        org.apache.spark.sql.functions.stddev(value)).first();
                double[] quantiles = data.stat().approxQuantile(feature, new double[]{0.25, 0.5, 0.75}, 0.01);
                Map<String, Object> numeric = new LinkedHashMap<>();
                numeric.put("min", stats.isNullAt(0) ? null : ((Number) stats.get(0)).doubleValue());
                numeric.put("max", stats.isNullAt(1) ? null : ((Number) stats.get(1)).doubleValue());
                numeric.put("mean", stats.isNullAt(2) ? null : ((Number) stats.get(2)).doubleValue());
                numeric.put("stddev", stats.isNullAt(3) ? null : ((Number) stats.get(3)).doubleValue());
                if (quantiles.length == 3) {
                    numeric.put("q1", quantiles[0]);
                    numeric.put("median", quantiles[1]);
                    numeric.put("q3", quantiles[2]);
                    double lower = quantiles[0] - 1.5 * (quantiles[2] - quantiles[0]);
                    double upper = quantiles[2] + 1.5 * (quantiles[2] - quantiles[0]);
                    long outliers = data.filter(value.lt(lower).or(value.gt(upper))).count();
                    numeric.put("iqrOutlierCount", outliers);
                    numeric.put("iqrOutlierRate", ratio(outliers, rowCount - missingCount));
                    if (ratio(outliers, rowCount - missingCount) > 0.05) warnings.add(feature + ": more than 5% IQR outliers");
                }
                item.put("numeric", numeric);
            } else {
                List<Map<String, Object>> topValues = new ArrayList<>();
                for (Row row : data.groupBy(value).count().orderBy(org.apache.spark.sql.functions.desc("count")).limit(10).collectAsList()) {
                    Map<String, Object> top = new LinkedHashMap<>();
                    top.put("value", row.isNullAt(0) ? null : String.valueOf(row.get(0)));
                    top.put("count", row.getLong(1));
                    topValues.add(top);
                }
                item.put("topValues", topValues);
                item.put("highCardinality", distinctCount > 1000);
                if (distinctCount > 1000) warnings.add(feature + ": high cardinality (" + distinctCount + ")");
            }
            if (item.get("constant").equals(Boolean.TRUE)) warnings.add(feature + ": constant feature");
            if (ratio(missingCount, rowCount) > 0.20) warnings.add(feature + ": missing rate exceeds 20%");
            profiles.add(item);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("rows", rowCount);
        result.put("columns", data.columns().length);
        result.put("featureCount", features.size());
        result.put("duplicateRows", rowCount - distinctRows);
        result.put("datasetFingerprint", fingerprint(jsonRows));
        result.put("features", profiles);
        result.put("warnings", warnings);
        if (label != null && columns.contains(label)) {
            List<Map<String, Object>> distribution = new ArrayList<>();
            for (Row row : data.groupBy(org.apache.spark.sql.functions.col(label)).count()
                    .orderBy(org.apache.spark.sql.functions.desc("count")).limit(100).collectAsList()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("label", row.isNullAt(0) ? null : String.valueOf(row.get(0)));
                item.put("count", row.getLong(1));
                item.put("rate", ratio(row.getLong(1), rowCount));
                distribution.add(item);
            }
            result.put("labelDistribution", distribution);
        }
        result.put("profilingMethod", "Spark SQL exact counts, approximate 1% quantiles, Tukey 1.5xIQR outlier rule");
        return result;
    }

    private static double ratio(long numerator, long denominator) {
        return denominator == 0 ? 0.0 : (double) numerator / denominator;
    }

    private static String fingerprint(List<String> rows) {
        try {
            List<String> canonical = new ArrayList<>(rows);
            Collections.sort(canonical);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String row : canonical) {
                digest.update(row.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) '\n');
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception error) {
            throw new IllegalStateException("Unable to fingerprint dataset", error);
        }
    }
}
