package io.sbase.server.automl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.ml.*;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.*;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.regression.*;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Reproducible Spark AutoML with leakage-aware temporal validation,
 * missing-value treatment, baseline comparison, fairness diagnostics and model cards.
 */
public final class AutoMlService {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> NUMERIC_TYPES =
            Set.of("byte", "short", "integer", "long", "float", "double", "decimal");

    private final SparkSession spark;

    public AutoMlService(SparkSession spark) {
        this.spark = Objects.requireNonNull(spark);
    }

    public Map<String, Object> train(Map<String, Object> request) {
        return train(request, null);
    }

    public Map<String, Object> train(Map<String, Object> request, String modelPath) {
        String task = String.valueOf(request.getOrDefault("task", "classification")).toLowerCase(Locale.ROOT);
        String label = String.valueOf(request.getOrDefault("label", "label"));
        List<String> features = new ArrayList<>(((List<?>) request.getOrDefault("features", List.of()))
                .stream().map(String::valueOf).toList());
        List<?> suppliedRows = (List<?>) request.getOrDefault("data", List.of());
        boolean timeSeries = task.equals("time_series");
        boolean classification = task.equals("classification");
        if (!Set.of("classification", "regression", "time_series").contains(task))
            throw new IllegalArgumentException("task must be classification, regression or time_series");
        if (features.isEmpty() || suppliedRows.size() < 50)
            throw new IllegalArgumentException("AutoML requires features and at least 50 rows");
        if (suppliedRows.size() > 100000)
            throw new IllegalArgumentException("AutoML inline data limit is 100000 rows");
        if (new HashSet<>(features).size() != features.size() || features.contains(label))
            throw new IllegalArgumentException("Feature columns must be unique and cannot include label");

        String sensitiveColumn = optionalString(request.get("sensitiveColumn"));
        if (sensitiveColumn != null && features.contains(sensitiveColumn))
            throw new IllegalArgumentException("sensitiveColumn must not be included in model features");

        List<String> jsonRows = new ArrayList<>(suppliedRows.size());
        for (Object row : suppliedRows) {
            if (!(row instanceof Map<?, ?> map) || !map.containsKey(label))
                throw new IllegalArgumentException("Every row must contain label");
            for (String feature : features) {
                if (!map.containsKey(feature))
                    throw new IllegalArgumentException("Missing feature: " + feature);
            }
            if (sensitiveColumn != null && !map.containsKey(sensitiveColumn))
                throw new IllegalArgumentException("Missing sensitiveColumn: " + sensitiveColumn);
            jsonRows.add(JSON.valueToTree(row).toString());
        }

        String fingerprint = fingerprint(suppliedRows);
        long seed = Math.max(0L, Math.min(9_223_372_036_854_775_000L,
                ((Number) request.getOrDefault("seed", 42)).longValue()));
        Dataset<Row> raw = spark.read().json(spark.createDataset(jsonRows, Encoders.STRING()));
        Set<String> columns = new HashSet<>(Arrays.asList(raw.columns()));
        if (!columns.contains(label) || !columns.containsAll(features))
            throw new IllegalArgumentException("Unknown label or feature column");
        if (raw.filter(col(label).isNull()).limit(1).count() > 0)
            throw new IllegalArgumentException("Label column cannot contain null values");
        String labelType = raw.schema().apply(label).dataType().typeName().toLowerCase(Locale.ROOT);
        if (!NUMERIC_TYPES.contains(labelType))
            throw new IllegalArgumentException("Classification and regression labels must be numeric");

        String timeColumn = null;
        int lags = 0;
        if (timeSeries) {
            timeColumn = String.valueOf(request.getOrDefault("timeColumn", ""));
            lags = Math.max(1, Math.min(24, ((Number) request.getOrDefault("lags", 3)).intValue()));
            if (timeColumn.isBlank() || !columns.contains(timeColumn))
                throw new IllegalArgumentException("time_series requires a valid timeColumn");
            if (!NUMERIC_TYPES.contains(raw.schema().apply(label).dataType().typeName().toLowerCase(Locale.ROOT)))
                throw new IllegalArgumentException("time_series label must be numeric");
            if (suppliedRows.size() < 50 + lags)
                throw new IllegalArgumentException("time_series requires at least 50 rows after lag generation");
            if (raw.filter(col(timeColumn).isNull()).limit(1).count() > 0)
                throw new IllegalArgumentException("timeColumn cannot contain null values");
            if (raw.groupBy(col(timeColumn)).count().filter(col("count").gt(1)).limit(1).count() > 0)
                throw new IllegalArgumentException("timeColumn values must be unique to prevent temporal leakage");

            WindowSpec order = Window.orderBy(col(timeColumn).asc());
            for (int lag = 1; lag <= lags; lag++) {
                String lagColumn = label + "__lag_" + lag;
                raw = raw.withColumn(lagColumn, lag(col(label), lag).over(order));
                if (!features.contains(lagColumn)) features.add(lagColumn);
            }
            raw = raw.filter(col(label + "__lag_1").isNotNull()).orderBy(col(timeColumn).asc());
        }

        Dataset<Row> profileData = raw;
        long rowCount = profileData.count();
        if (rowCount < 50) throw new IllegalArgumentException("At least 50 usable rows are required after preprocessing");

        List<String> selectedFeatures = new ArrayList<>();
        List<String> droppedFeatures = new ArrayList<>();
        double varianceThreshold = Math.max(0.0,
                ((Number) request.getOrDefault("varianceThreshold", 1.0e-12)).doubleValue());
        for (String feature : features) {
            String type = profileData.schema().apply(feature).dataType().typeName().toLowerCase(Locale.ROOT);
            boolean useful;
            if (NUMERIC_TYPES.contains(type)) {
                Row stats = profileData.agg(variance(col(feature))).first();
                useful = !stats.isNullAt(0) && Double.isFinite(stats.getDouble(0))
                        && stats.getDouble(0) > varianceThreshold;
            } else {
                useful = profileData.select(col(feature)).where(col(feature).isNotNull()).distinct().limit(2).count() > 1;
            }
            if (useful) selectedFeatures.add(feature);
            else droppedFeatures.add(feature);
        }
        features = selectedFeatures;
        if (features.isEmpty()) throw new IllegalArgumentException("Feature selection removed every constant or empty feature");

        // Spark Imputer expects NaN rather than SQL NULL; preserve profileData for accurate missingness reports.
        raw = profileData;
        for (String feature : features) {
            String type = raw.schema().apply(feature).dataType().typeName().toLowerCase(Locale.ROOT);
            if (NUMERIC_TYPES.contains(type)) {
                raw = raw.withColumn(feature, when(col(feature).isNull(), lit(Double.NaN)).otherwise(col(feature)));
            }
        }

        List<PipelineStage> preprocessing = new ArrayList<>();
        List<String> assemblerInputs = new ArrayList<>();
        List<String> numericInputs = new ArrayList<>();
        List<String> numericOutputs = new ArrayList<>();
        List<String> indexedColumns = new ArrayList<>();
        List<String> encodedColumns = new ArrayList<>();

        for (String feature : features) {
            String type = raw.schema().apply(feature).dataType().typeName().toLowerCase(Locale.ROOT);
            if (NUMERIC_TYPES.contains(type)) {
                numericInputs.add(feature);
                String imputed = feature + "__sbase_imputed";
                numericOutputs.add(imputed);
                assemblerInputs.add(imputed);
            } else {
                String indexed = feature + "__sbase_index";
                String encoded = feature + "__sbase_ohe";
                preprocessing.add(new StringIndexer().setInputCol(feature).setOutputCol(indexed).setHandleInvalid("keep"));
                indexedColumns.add(indexed);
                encodedColumns.add(encoded);
                assemblerInputs.add(encoded);
            }
        }
        if (!numericInputs.isEmpty()) {
            preprocessing.add(new Imputer().setInputCols(numericInputs.toArray(String[]::new))
                    .setOutputCols(numericOutputs.toArray(String[]::new)).setStrategy("median"));
        }
        if (!indexedColumns.isEmpty()) {
            preprocessing.add(new OneHotEncoder().setInputCols(indexedColumns.toArray(String[]::new))
                    .setOutputCols(encodedColumns.toArray(String[]::new)).setHandleInvalid("keep"));
        }
        preprocessing.add(new VectorAssembler().setInputCols(assemblerInputs.toArray(String[]::new))
                .setOutputCol("features").setHandleInvalid("error"));

        Dataset<Row> train;
        Dataset<Row> validation;
        String splitStrategy;
        if (timeSeries) {
            long trainingCount = Math.max(2L, Math.min(rowCount - 2, (long) Math.floor(rowCount * 0.8)));
            train = raw.orderBy(col(timeColumn).asc()).limit((int) trainingCount);
            validation = raw.orderBy(col(timeColumn).desc()).limit((int) (rowCount - trainingCount))
                    .orderBy(col(timeColumn).asc());
            splitStrategy = "chronological_holdout_80_20";
        } else {
            Dataset<Row>[] split = raw.randomSplit(new double[]{0.8, 0.2}, seed);
            train = split[0];
            validation = split[1];
            splitStrategy = "deterministic_random_holdout_80_20";
        }
        if (train.limit(2).count() < 2 || validation.limit(2).count() < 2)
            throw new IllegalArgumentException("Not enough rows after validation split");

        String requestedMetric = String.valueOf(request.getOrDefault("optimizationMetric",
                classification ? "f1" : "rmse")).toLowerCase(Locale.ROOT);
        validateMetric(task, requestedMetric);
        String metric = switch (requestedMetric) {
            case "weightedprecision" -> "weightedPrecision";
            case "weightedrecall" -> "weightedRecall";
            default -> requestedMetric;
        };
        boolean higherIsBetter = classification || metric.equals("r2") || metric.equals("var");

        List<PipelineStage> candidates = new ArrayList<>();
        if (classification) {
            candidates.add(new LogisticRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(100).setRegParam(0.01));
            candidates.add(new RandomForestClassifier().setLabelCol(label).setFeaturesCol("features").setNumTrees(100).setMaxDepth(8).setSeed(seed));
            candidates.add(new DecisionTreeClassifier().setLabelCol(label).setFeaturesCol("features").setMaxDepth(8).setSeed(seed));
        } else {
            candidates.add(new LinearRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(100).setRegParam(0.01));
            candidates.add(new RandomForestRegressor().setLabelCol(label).setFeaturesCol("features").setNumTrees(100).setMaxDepth(8).setSeed(seed));
            candidates.add(new DecisionTreeRegressor().setLabelCol(label).setFeaturesCol("features").setMaxDepth(8).setSeed(seed));
            if (timeSeries)
                candidates.add(new GBTRegressor().setLabelCol(label).setFeaturesCol("features").setMaxIter(80).setMaxDepth(5).setSeed(seed));
        }

        int maxCandidates = Math.max(1, Math.min(4,
                ((Number) request.getOrDefault("maxCandidates", timeSeries ? 4 : 3)).intValue()));
        int folds = timeSeries ? 0 : Math.max(2, Math.min(5,
                ((Number) request.getOrDefault("cvFolds", 3)).intValue()));
        int cvParallelism = Math.max(1, Math.min(4,
                ((Number) request.getOrDefault("cvParallelism", 2)).intValue()));
        double bestScore = higherIsBetter ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        PipelineModel best = null;
        String bestAlgorithm = "";
        Map<String, Object> bestMetrics = Map.of();
        List<Map<String, Object>> trials = new ArrayList<>();

        for (PipelineStage estimator : candidates.stream().limit(maxCandidates).toList()) {
            String algorithm = estimator.getClass().getSimpleName();
            long started = System.nanoTime();
            try {
                List<PipelineStage> stages = new ArrayList<>(preprocessing);
                stages.add(estimator);
                Pipeline pipeline = new Pipeline().setStages(stages.toArray(PipelineStage[]::new));
                PipelineModel model;
                double cvAverage;
                if (timeSeries) {
                    // Spark CrossValidator uses random folds; never use it for ordered temporal observations.
                    model = pipeline.fit(train);
                    cvAverage = Double.NaN;
                } else {
                    CrossValidator validator = new CrossValidator().setEstimator(pipeline)
                            .setEvaluator(evaluator(label, classification, metric))
                            .setEstimatorParamMaps(parameterGrid(estimator)).setNumFolds(folds)
                            .setParallelism(cvParallelism).setSeed(seed);
                    CrossValidatorModel cvModel = validator.fit(train);
                    model = (PipelineModel) cvModel.bestModel();
                    cvAverage = Arrays.stream(cvModel.avgMetrics()).average().orElse(Double.NaN);
                }
                Dataset<Row> predictions = model.transform(validation);
                Map<String, Object> metrics = evaluate(predictions, label, classification);
                double score = ((Number) metrics.get(metric)).doubleValue();
                Map<String, Object> trial = new LinkedHashMap<>();
                trial.put("algorithm", algorithm);
                trial.put("metric", metric);
                trial.put("score", score);
                trial.put("metrics", metrics);
                trial.put("cvFolds", folds);
                if (Double.isFinite(cvAverage)) trial.put("cvAverageMetric", cvAverage);
                trial.put("status", "SUCCEEDED");
                trial.put("durationMs", nanosToMillis(System.nanoTime() - started));
                trials.add(trial);
                if (best == null || (higherIsBetter ? score > bestScore : score < bestScore)) {
                    best = model;
                    bestScore = score;
                    bestAlgorithm = algorithm;
                    bestMetrics = metrics;
                }
            } catch (Exception error) {
                Map<String, Object> trial = new LinkedHashMap<>();
                trial.put("algorithm", algorithm);
                trial.put("status", "FAILED");
                trial.put("durationMs", nanosToMillis(System.nanoTime() - started));
                trial.put("error", error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage()));
                trials.add(trial);
            }
        }
        if (best == null) throw new IllegalStateException("All AutoML candidates failed: " + trials);

        Dataset<Row> bestPredictions = best.transform(validation);
        Map<String, Object> baseline = baselineMetrics(train, validation, label, classification);
        Map<String, Object> fairness = fairnessReport(bestPredictions, label, sensitiveColumn, classification, request);
        Map<String, Object> quality = dataQuality(profileData, features);
        Map<String, Object> profile = featureProfile(profileData, features);
        Map<String, Object> reproducibility = new LinkedHashMap<>();
        reproducibility.put("datasetFingerprintSha256", fingerprint);
        reproducibility.put("seed", seed);
        reproducibility.put("splitStrategy", splitStrategy);
        reproducibility.put("crossValidationFolds", folds);
        reproducibility.put("sparkVersion", spark.version());
        reproducibility.put("createdAt", Instant.now().toString());
        reproducibility.put("inputRows", suppliedRows.size());
        reproducibility.put("usableRows", rowCount);
        reproducibility.put("schema", raw.schema().json());

        Map<String, Object> modelCard = new LinkedHashMap<>();
        modelCard.put("task", task);
        modelCard.put("label", label);
        modelCard.put("features", features);
        modelCard.put("optimizationMetric", metric);
        modelCard.put("optimizationDirection", higherIsBetter ? "maximize" : "minimize");
        modelCard.put("validationMetrics", bestMetrics);
        modelCard.put("baselineMetrics", baseline);
        modelCard.put("dataQuality", quality);
        modelCard.put("featureProfile", profile);
        modelCard.put("fairness", fairness);
        modelCard.put("reproducibility", reproducibility);
        modelCard.put("validationProtocol", splitStrategy);
        modelCard.put("limitations", timeSeries
                ? List.of("Temporal holdout is chronological; rolling-origin cross-validation is not enabled.")
                : List.of("Holdout is deterministic; external temporal or group leakage checks depend on dataset semantics."));

        String modelUri = null;
        if (modelPath != null && !modelPath.isBlank()) {
            try {
                best.write().overwrite().save(modelPath);
                modelUri = modelPath;
            } catch (Exception error) {
                throw new IllegalStateException("Model persistence failed: " + error.getMessage(), error);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "SUCCEEDED");
        result.put("task", task);
        result.put("algorithm", bestAlgorithm);
        result.put("metric", metric);
        result.put("score", bestScore);
        result.put("metrics", bestMetrics);
        result.put("baselineMetrics", baseline);
        result.put("trainingRows", train.count());
        result.put("validationRows", validation.count());
        result.put("features", features);
        result.put("droppedConstantFeatures", droppedFeatures);
        result.put("categoricalEncoding", "StringIndexer + OneHotEncoder");
        result.put("numericMissingValueTreatment", "median imputation fitted inside each training fold");
        result.put("featureSelection", "variance threshold for numeric and cardinality filter for categorical columns");
        result.put("timeSeriesLagFeatures", timeSeries);
        result.put("trials", trials);
        result.put("modelCard", modelCard);
        result.put("modelFormat", "Spark PipelineModel");
        if (modelUri != null) result.put("modelUri", modelUri);
        return result;
    }

    private static void validateMetric(String task, String metric) {
        String normalized = metric.toLowerCase(Locale.ROOT);
        Set<String> supported = task.equals("classification")
                ? Set.of("accuracy", "f1", "weightedprecision", "weightedrecall")
                : Set.of("rmse", "mae", "mse", "r2", "var");
        if (!supported.contains(normalized))
            throw new IllegalArgumentException("Unsupported optimizationMetric for " + task + ": " + metric);
    }

    private static Evaluator evaluator(String label, boolean classification, String metric) {
        return classification
                ? new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName(metric)
                : new RegressionEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName(metric);
    }

    private static Map<String, Object> evaluate(Dataset<Row> predictions, String label, boolean classification) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (classification) {
            for (String metric : List.of("accuracy", "f1", "weightedPrecision", "weightedRecall")) {
                double value = new MulticlassClassificationEvaluator().setLabelCol(label)
                        .setPredictionCol("prediction").setMetricName(metric).evaluate(predictions);
                result.put(metric, value);
            }
            if (predictions.select(col(label)).distinct().limit(3).count() == 2) {
                try {
                    result.put("areaUnderROC", new BinaryClassificationEvaluator().setLabelCol(label)
                            .setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC").evaluate(predictions));
                    result.put("areaUnderPR", new BinaryClassificationEvaluator().setLabelCol(label)
                            .setRawPredictionCol("rawPrediction").setMetricName("areaUnderPR").evaluate(predictions));
                } catch (Exception ignored) {
                    // Some estimators do not expose a compatible binary raw-prediction vector.
                }
            }
        } else {
            for (String metric : List.of("rmse", "mae", "mse", "r2", "var")) {
                double value = new RegressionEvaluator().setLabelCol(label)
                        .setPredictionCol("prediction").setMetricName(metric).evaluate(predictions);
                if (Double.isFinite(value)) result.put(metric, value);
            }
        }
        return result;
    }

    private static Map<String, Object> baselineMetrics(Dataset<Row> train, Dataset<Row> validation,
                                                         String label, boolean classification) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (classification) {
            Row majority = train.groupBy(col(label)).count().orderBy(desc("count")).first();
            Object majorityLabel = majority.get(0);
            long total = validation.count();
            long correct = validation.filter(col(label).equalTo(lit(majorityLabel))).count();
            result.put("baseline", "majority_class");
            result.put("accuracy", total == 0 ? 0.0 : (double) correct / total);
        } else {
            double mean = train.agg(avg(col(label))).first().getDouble(0);
            Row errors = validation.agg(
                    sqrt(avg(pow(col(label).minus(mean), 2))).alias("rmse"),
                    avg(abs(col(label).minus(mean))).alias("mae")).first();
            result.put("baseline", "training_mean");
            result.put("rmse", errors.getDouble(0));
            result.put("mae", errors.getDouble(1));
        }
        return result;
    }

    private static Map<String, Object> fairnessReport(Dataset<Row> predictions, String label,
                                                        String sensitiveColumn, boolean classification,
                                                        Map<String, Object> request) {
        Map<String, Object> report = new LinkedHashMap<>();
        if (sensitiveColumn == null) {
            report.put("status", "NOT_REQUESTED");
            return report;
        }
        if (!classification || predictions.select(col(label)).distinct().limit(3).count() != 2) {
            report.put("status", "UNSUPPORTED");
            report.put("reason", "Fairness metrics currently require binary classification.");
            return report;
        }
        double positiveLabel = ((Number) request.getOrDefault("positiveLabel", 1)).doubleValue();
        int minimumGroupSize = Math.max(2, Math.min(10000,
                ((Number) request.getOrDefault("fairnessMinGroupSize", 5)).intValue()));
        List<Map<String, Object>> groups = new ArrayList<>();
        Dataset<Row> grouped = predictions.groupBy(col(sensitiveColumn)).agg(
                count(lit(1)).alias("rows"),
                avg(when(col("prediction").equalTo(positiveLabel), 1.0).otherwise(0.0)).alias("selectionRate"),
                avg(when(col(label).equalTo(positiveLabel).and(col("prediction").equalTo(positiveLabel)), 1.0)
                        .otherwise(0.0)).alias("truePositiveRate"));
        double minSelection = Double.POSITIVE_INFINITY, maxSelection = Double.NEGATIVE_INFINITY;
        double minTpr = Double.POSITIVE_INFINITY, maxTpr = Double.NEGATIVE_INFINITY;
        for (Row row : grouped.collectAsList()) {
            long count = row.getAs("rows");
            if (count < minimumGroupSize) continue;
            double selection = ((Number) row.getAs("selectionRate")).doubleValue();
            double tpr = ((Number) row.getAs("truePositiveRate")).doubleValue();
            Map<String, Object> group = new LinkedHashMap<>();
            group.put("group", String.valueOf(row.get(0)));
            group.put("rows", count);
            group.put("selectionRate", selection);
            group.put("truePositiveRate", tpr);
            groups.add(group);
            minSelection = Math.min(minSelection, selection);
            maxSelection = Math.max(maxSelection, selection);
            minTpr = Math.min(minTpr, tpr);
            maxTpr = Math.max(maxTpr, tpr);
        }
        report.put("status", groups.size() < 2 ? "INSUFFICIENT_GROUPS" : "COMPUTED");
        report.put("positiveLabel", positiveLabel);
        report.put("minimumGroupSize", minimumGroupSize);
        report.put("groups", groups);
        if (groups.size() >= 2) {
            report.put("demographicParityDifference", maxSelection - minSelection);
            report.put("equalOpportunityDifference", maxTpr - minTpr);
        }
        report.put("interpretation", "Diagnostic disparities are not proof of discrimination or legal compliance.");
        return report;
    }

    private static Map<String, Object> dataQuality(Dataset<Row> data, List<String> features) {
        long rows = data.count();
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Object> missing = new LinkedHashMap<>();
        for (String feature : features) {
            long count = data.filter(col(feature).isNull()).count();
            missing.put(feature, Map.of("missingCount", count,
                    "missingRate", rows == 0 ? 0.0 : (double) count / rows));
        }
        result.put("rowCount", rows);
        result.put("featureMissingness", missing);
        return result;
    }

    private static Map<String, Object> featureProfile(Dataset<Row> data, List<String> features) {
        long rows = data.count();
        Map<String, Object> profile = new LinkedHashMap<>();
        for (String feature : features) {
            String type = data.schema().apply(feature).dataType().typeName().toLowerCase(Locale.ROOT);
            Map<String, Object> stats = new LinkedHashMap<>();
            long missing = data.filter(col(feature).isNull()).count();
            stats.put("missingRate", rows == 0 ? 0.0 : (double) missing / rows);
            if (NUMERIC_TYPES.contains(type)) {
                Row values = data.agg(avg(col(feature)), stddev(col(feature)), min(col(feature)), max(col(feature))).first();
                stats.put("numeric", true);
                stats.put("mean", values.isNullAt(0) ? 0.0 : ((Number) values.get(0)).doubleValue());
                stats.put("stddev", values.isNullAt(1) ? 0.0 : ((Number) values.get(1)).doubleValue());
                stats.put("min", values.isNullAt(2) ? 0.0 : ((Number) values.get(2)).doubleValue());
                stats.put("max", values.isNullAt(3) ? 0.0 : ((Number) values.get(3)).doubleValue());
            } else {
                List<String> known = data.select(col(feature).cast("string")).where(col(feature).isNotNull())
                        .distinct().limit(1001).collectAsList().stream().map(row -> row.getString(0)).toList();
                stats.put("numeric", false);
                stats.put("cardinality", data.select(col(feature)).distinct().count());
                stats.put("knownCategories", known.size() > 1000 ? known.subList(0, 1000) : known);
                stats.put("knownCategoriesTruncated", known.size() > 1000);
            }
            profile.put(feature, stats);
        }
        return profile;
    }

    private static String fingerprint(List<?> rows) {
        try {
            List<String> canonical = new ArrayList<>(rows.size());
            for (Object row : rows) {
                if (row instanceof Map<?, ?> map) {
                    TreeMap<String, Object> sorted = new TreeMap<>();
                    map.forEach((key, value) -> sorted.put(String.valueOf(key), value));
                    canonical.add(JSON.writeValueAsString(sorted));
                } else canonical.add(JSON.writeValueAsString(row));
            }
            Collections.sort(canonical);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(String.join("\n", canonical).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (Exception error) {
            throw new IllegalArgumentException("Could not fingerprint training dataset", error);
        }
    }

    private static String optionalString(Object value) {
        if (value == null) return null;
        String text = String.valueOf(value).trim();
        return text.isEmpty() || text.equalsIgnoreCase("null") ? null : text;
    }

    private static long nanosToMillis(long nanos) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static org.apache.spark.ml.param.ParamMap[] parameterGrid(PipelineStage estimator) {
        ParamGridBuilder grid = new ParamGridBuilder();
        if (estimator instanceof LogisticRegression model)
            grid.addGrid(model.regParam(), new double[]{0.001, 0.01, 0.1})
                    .addGrid(model.elasticNetParam(), new double[]{0.0, 0.5});
        else if (estimator instanceof LinearRegression model)
            grid.addGrid(model.regParam(), new double[]{0.001, 0.01, 0.1});
        else if (estimator instanceof RandomForestClassifier model)
            grid.addGrid(model.numTrees(), new int[]{50, 100}).addGrid(model.maxDepth(), new int[]{5, 8});
        else if (estimator instanceof RandomForestRegressor model)
            grid.addGrid(model.numTrees(), new int[]{50, 100}).addGrid(model.maxDepth(), new int[]{5, 8});
        else if (estimator instanceof DecisionTreeClassifier model)
            grid.addGrid(model.maxDepth(), new int[]{4, 8, 12});
        else if (estimator instanceof DecisionTreeRegressor model)
            grid.addGrid(model.maxDepth(), new int[]{4, 8, 12});
        else if (estimator instanceof GBTRegressor model)
            grid.addGrid(model.maxIter(), new int[]{40, 80}).addGrid(model.maxDepth(), new int[]{3, 5});
        return grid.build();
    }
}
