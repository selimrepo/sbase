package io.sbase.server.automl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Durable experiment lifecycle, model cards and production drift history backed by PostgreSQL. */
public final class AutoMlLifecycleService {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final SparkSession spark;
    private final String jdbcUrl = env("SBASE_POSTGRES_URL", "jdbc:postgresql://localhost:5432/sbase");
    private final String user = env("SBASE_POSTGRES_USER", "sbase");
    private final String password = env("SBASE_POSTGRES_PASSWORD", "");
    private final Path registryRoot = Path.of(env("SBASE_MODEL_REGISTRY_PATH", "/var/lib/sbase/models"))
            .toAbsolutePath().normalize();

    public AutoMlLifecycleService(SparkSession spark) {
        this.spark = Objects.requireNonNull(spark);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> train(Map<String, Object> request) throws Exception {
        UUID experimentId = UUID.randomUUID();
        String name = String.valueOf(request.getOrDefault("name", "AutoML-" + experimentId.toString().substring(0, 8)));
        String task = String.valueOf(request.getOrDefault("task", "classification")).toUpperCase(Locale.ROOT);
        String label = String.valueOf(request.getOrDefault("label", "label"));
        List<?> features = (List<?>) request.getOrDefault("features", List.of());
        if (name.length() > 200) throw new IllegalArgumentException("Experiment name is limited to 200 characters");
        if (!Set.of("CLASSIFICATION", "REGRESSION", "TIME_SERIES").contains(task))
            throw new IllegalArgumentException("Unsupported AutoML task");

        String modelPath = registryRoot.resolve(experimentId.toString()).toString();
        insertExperiment(experimentId, name, task, label, JSON.writeValueAsString(features));
        updateStatus(experimentId, "RUNNING", null, null, null, null);
        long started = System.nanoTime();
        try {
            Map<String, Object> result = new AutoMlService(spark).train(request, modelPath);
            String algorithm = String.valueOf(result.get("algorithm"));
            String metric = String.valueOf(result.get("metric"));
            double score = ((Number) result.get("score")).doubleValue();
            List<Map<String, Object>> trials = (List<Map<String, Object>>) result.get("trials");
            Map<String, Object> modelCard = (Map<String, Object>) result.get("modelCard");
            UUID modelId = UUID.randomUUID();
            completeTraining(experimentId, modelId, name, algorithm, task, metric, score,
                    modelPath, features, trials, modelCard);
            Map<String, Object> response = new LinkedHashMap<>(result);
            response.put("experimentId", experimentId.toString());
            response.put("modelId", modelId.toString());
            response.put("modelUri", modelPath);
            response.put("durationMs", java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            return response;
        } catch (Exception error) {
            updateFailure(experimentId, error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage()));
            throw error;
        }
    }

    public List<Map<String, Object>> experiments(int limit) throws SQLException {
        String sql = "SELECT id,name,task_type,label_column,status,best_algorithm,best_metric,best_score,model_uri,metadata,created_at,finished_at FROM automl_experiment ORDER BY created_at DESC LIMIT ?";
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setInt(1, Math.max(1, Math.min(limit, 200)));
            try (ResultSet r = p.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (r.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id", r.getObject("id").toString());
                    row.put("name", r.getString("name"));
                    row.put("task", r.getString("task_type"));
                    row.put("label", r.getString("label_column"));
                    row.put("status", r.getString("status"));
                    row.put("algorithm", r.getString("best_algorithm"));
                    row.put("metric", r.getString("best_metric"));
                    row.put("score", r.getObject("best_score"));
                    row.put("modelUri", r.getString("model_uri"));
                    row.put("metadata", parseJson(r.getString("metadata")));
                    row.put("createdAt", r.getTimestamp("created_at").toInstant().toString());
                    Timestamp finished = r.getTimestamp("finished_at");
                    row.put("finishedAt", finished == null ? null : finished.toInstant().toString());
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    public Map<String, Object> model(UUID id) throws SQLException {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "SELECT id,experiment_id,name,algorithm,task_type,metric,best_score,model_uri,version,status,model_card,created_at FROM model_registry WHERE id=?")) {
            p.setObject(1, id);
            try (ResultSet r = p.executeQuery()) {
                if (!r.next()) throw new NoSuchElementException("Model not found");
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("id", r.getObject("id").toString());
                m.put("experimentId", r.getObject("experiment_id").toString());
                m.put("name", r.getString("name"));
                m.put("algorithm", r.getString("algorithm"));
                m.put("task", r.getString("task_type"));
                m.put("metric", r.getString("metric"));
                m.put("score", r.getDouble("best_score"));
                m.put("modelUri", r.getString("model_uri"));
                m.put("version", r.getInt("version"));
                m.put("status", r.getString("status"));
                m.put("modelCard", parseJson(r.getString("model_card")));
                m.put("createdAt", r.getTimestamp("created_at").toInstant().toString());
                return m;
            }
        }
    }

    public Map<String, Object> predict(UUID modelId, List<Map<String, Object>> rows) throws Exception {
        if (rows == null || rows.isEmpty() || rows.size() > 50000)
            throw new IllegalArgumentException("Prediction requires 1..50000 rows");
        Map<String, Object> metadata = model(modelId);
        if (!"READY".equals(metadata.get("status")))
            throw new IllegalStateException("Only READY models can serve predictions");
        String path = String.valueOf(metadata.get("modelUri"));
        Path resolved = Path.of(path).toAbsolutePath().normalize();
        if (!resolved.startsWith(registryRoot) || !Files.exists(resolved))
            throw new IllegalStateException("Model artifact is missing or outside registry root");
        PipelineModel pipeline = PipelineModel.load(resolved.toString());
        List<String> jsonRows = rows.stream().map(row -> JSON.valueToTree(row).toString()).toList();
        Dataset<Row> input = spark.read().json(spark.createDataset(jsonRows, Encoders.STRING()));
        List<String> predictions = pipeline.transform(input).toJSON().collectAsList();
        return Map.of("modelId", modelId.toString(), "predictions", predictions, "rowCount", predictions.size());
    }

    public Map<String, Object> monitor(UUID modelId, List<Map<String, Object>> rows) throws SQLException {
        return monitor(modelId, rows, Map.of(), null);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> monitor(UUID modelId, List<Map<String, Object>> rows,
                                       Map<String, Object> policy,
                                       Map<String, Object> retrainingRequest) throws SQLException {
        Map<String, Object> metadata = model(modelId);
        if (!"READY".equals(metadata.get("status")))
            throw new IllegalStateException("Only READY models can be monitored");
        Map<String, Object> card = (Map<String, Object>) metadata.get("modelCard");
        Map<String, Object> report = ModelDriftMonitor.evaluate(card, rows);
        Map<String, Object> automation = new LinkedHashMap<>();
        String driftLevel = String.valueOf(report.get("driftLevel"));
        boolean autoRetrain = Boolean.TRUE.equals(policy.get("autoRetrain"));
        String triggerLevel = String.valueOf(policy.getOrDefault("triggerLevel", "HIGH")).toUpperCase(Locale.ROOT);
        if (!Set.of("LOW", "MEDIUM", "HIGH").contains(triggerLevel))
            throw new IllegalArgumentException("triggerLevel must be LOW, MEDIUM or HIGH");

        if (!autoRetrain) {
            automation.put("status", "DISABLED");
        } else if (!ModelPromotionPolicy.shouldTrigger(driftLevel, triggerLevel)) {
            automation.put("status", "NOT_TRIGGERED");
            automation.put("triggerLevel", triggerLevel);
        } else if (retrainingRequest == null || !hasLabeledTrainingData(retrainingRequest)) {
            automation.put("status", "SKIPPED_MISSING_LABELED_TRAINING_DATA");
            automation.put("reason", "Drift is not a substitute for labels; supply a complete retrainingRequest with labeled rows.");
        } else {
            int cooldownHours = Math.max(0, Math.min(720,
                    ((Number) policy.getOrDefault("cooldownHours", 24)).intValue()));
            if (cooldownHours > 0 && retrainingRecentlySucceeded(modelId, cooldownHours)) {
                automation.put("status", "SKIPPED_COOLDOWN");
                automation.put("cooldownHours", cooldownHours);
            } else {
                try {
                    String sourceTask = String.valueOf(metadata.get("task")).toUpperCase(Locale.ROOT);
                    String requestedTask = String.valueOf(retrainingRequest.getOrDefault("task", "classification"))
                            .toUpperCase(Locale.ROOT);
                    if (!sourceTask.equals(requestedTask))
                        throw new IllegalArgumentException("Retraining task must match the monitored model task");
                    Map<String, Object> candidate = train(new LinkedHashMap<>(retrainingRequest));
                    UUID candidateId = UUID.fromString(String.valueOf(candidate.get("modelId")));
                    automation.put("status", "RETRAINED");
                    automation.put("candidateModelId", candidateId.toString());
                    automation.put("candidateScore", candidate.get("score"));
                    automation.put("candidateMetric", candidate.get("metric"));

                    if (Boolean.TRUE.equals(policy.get("autoPromote"))) {
                        String alias = String.valueOf(policy.getOrDefault("promotionAlias", "")).trim();
                        if (alias.isEmpty())
                            throw new IllegalArgumentException("promotionAlias is required when autoPromote is true");
                        String sourceMetric = String.valueOf(metadata.get("metric"));
                        String candidateMetric = String.valueOf(candidate.get("metric"));
                        if (!sourceMetric.equals(candidateMetric)) {
                            automation.put("promotionStatus", "BLOCKED_METRIC_MISMATCH");
                        } else if (!(policy.get("promotionEvaluationData") instanceof List<?> evaluationRows)
                                || evaluationRows.size() < 2 || evaluationRows.size() > 50000) {
                            automation.put("promotionStatus", "BLOCKED_MISSING_COMMON_EVALUATION_DATA");
                            automation.put("reason", "Supply 2..50000 labeled promotionEvaluationData rows so incumbent and candidate are scored on the same holdout.");
                        } else {
                            Map<String, Object> candidateCard = (Map<String, Object>) candidate.get("modelCard");
                            if (!sameFeatureSchema(card, candidateCard)) {
                                automation.put("promotionStatus", "BLOCKED_FEATURE_SCHEMA_MISMATCH");
                            } else {
                                List<Map<String, Object>> commonRows = (List<Map<String, Object>>) evaluationRows;
                                double incumbentScore = evaluateModelOnRows(metadata, card, commonRows, sourceMetric);
                                double candidateScore = evaluateModelOnRows(
                                        Map.of("modelUri", candidate.get("modelUri"), "task", candidate.get("task")),
                                        candidateCard, commonRows, sourceMetric);
                                boolean higherIsBetter = "maximize".equalsIgnoreCase(
                                        String.valueOf(card.getOrDefault("optimizationDirection", "minimize")));
                                double improvement = ModelPromotionPolicy.improvement(
                                        candidateScore, incumbentScore, higherIsBetter);
                                double minimumImprovement = Math.max(0.0,
                                        ((Number) policy.getOrDefault("minimumImprovement", 0.0)).doubleValue());
                                automation.put("evaluationProtocol", "same_labeled_holdout_for_incumbent_and_candidate");
                                automation.put("incumbentEvaluationScore", incumbentScore);
                                automation.put("candidateEvaluationScore", candidateScore);
                                automation.put("improvement", improvement);
                                automation.put("minimumImprovement", minimumImprovement);
                                if (!ModelPromotionPolicy.shouldPromote(improvement, minimumImprovement)) {
                                    automation.put("promotionStatus", "BLOCKED_INSUFFICIENT_IMPROVEMENT");
                                } else if (promoteIfCurrent(alias, modelId, candidateId, sourceMetric,
                                        incumbentScore, candidateScore, improvement)) {
                                    automation.put("promotionStatus", "PROMOTED");
                                    automation.put("alias", alias);
                                } else {
                                    automation.put("promotionStatus", "SKIPPED_ALIAS_CHANGED");
                                    automation.put("reason", "The alias no longer points to the model that was monitored.");
                                }
                            }
                        }
                    } else {
                        automation.put("promotionStatus", "DISABLED");
                    }
                } catch (Exception error) {
                    String message = error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage());
                    if ("RETRAINED".equals(automation.get("status"))) {
                        automation.put("promotionStatus", "FAILED");
                        automation.put("promotionError", message);
                    } else {
                        automation.put("status", "FAILED");
                        automation.put("error", message);
                    }
                }
            }
        }
        report.put("automation", automation);
        UUID runId = UUID.randomUUID();
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "INSERT INTO model_monitoring_run(id,model_id,row_count,drift_level,report) VALUES(?,?,?,?,?::jsonb)")) {
            p.setObject(1, runId);
            p.setObject(2, modelId);
            p.setInt(3, rows.size());
            p.setString(4, driftLevel);
            p.setString(5, jsonString(report));
            p.executeUpdate();
        }
        Map<String, Object> result = new LinkedHashMap<>(report);
        result.put("runId", runId.toString());
        result.put("modelId", modelId.toString());
        return result;
    }

    @SuppressWarnings("unchecked")
    private double evaluateModelOnRows(Map<String, Object> metadata, Map<String, Object> card,
                                       List<Map<String, Object>> rows, String metric) throws Exception {
        String label = String.valueOf(card.get("label"));
        List<String> features = (List<String>) card.get("features");
        if (features == null || features.isEmpty())
            throw new IllegalStateException("Model card has no feature schema");
        List<String> jsonRows = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            if (!row.containsKey(label) || row.get(label) == null)
                throw new IllegalArgumentException("promotionEvaluationData must include non-null label: " + label);
            for (String feature : features) {
                if (!row.containsKey(feature))
                    throw new IllegalArgumentException("promotionEvaluationData is missing feature: " + feature);
            }
            jsonRows.add(JSON.valueToTree(row).toString());
        }
        String path = String.valueOf(metadata.get("modelUri"));
        Path resolved = Path.of(path).toAbsolutePath().normalize();
        if (!resolved.startsWith(registryRoot) || !Files.exists(resolved))
            throw new IllegalStateException("Model artifact is missing or outside registry root");
        Dataset<Row> input = spark.read().json(spark.createDataset(jsonRows, Encoders.STRING()));
        PipelineModel pipeline = PipelineModel.load(resolved.toString());
        Dataset<Row> predictions = pipeline.transform(input);
        boolean classification = "CLASSIFICATION".equalsIgnoreCase(String.valueOf(metadata.get("task")));
        Map<String, Object> metrics = AutoMlService.evaluate(predictions, label, classification);
        Object score = metrics.get(metric);
        if (!(score instanceof Number number) || !Double.isFinite(number.doubleValue()))
            throw new IllegalStateException("Common evaluation data did not produce metric " + metric);
        return number.doubleValue();
    }

    @SuppressWarnings("unchecked")
    private static boolean sameFeatureSchema(Map<String, Object> left, Map<String, Object> right) {
        List<String> leftFeatures = (List<String>) left.get("features");
        List<String> rightFeatures = (List<String>) right.get("features");
        return leftFeatures != null && rightFeatures != null
                && new HashSet<>(leftFeatures).equals(new HashSet<>(rightFeatures))
                && Objects.equals(left.get("label"), right.get("label"));
    }

    private static boolean hasLabeledTrainingData(Map<String, Object> request) {
        Object label = request.get("label");
        Object features = request.get("features");
        Object data = request.get("data");
        if (label == null || !(features instanceof List<?> featureList) || featureList.isEmpty()
                || !(data instanceof List<?> rows) || rows.size() < 50 || rows.size() > 100000)
            return false;
        String labelName = String.valueOf(label);
        for (Object row : rows) {
            if (!(row instanceof Map<?, ?> values) || !values.containsKey(labelName)
                    || values.get(labelName) == null) return false;
        }
        return true;
    }

    private boolean retrainingRecentlySucceeded(UUID modelId, int cooldownHours) throws SQLException {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "SELECT 1 FROM model_monitoring_run WHERE model_id=? AND created_at >= now() - (? * interval '1 hour') "
                        + "AND report->'automation'->>'status'='RETRAINED' LIMIT 1")) {
            p.setObject(1, modelId);
            p.setInt(2, cooldownHours);
            try (ResultSet r = p.executeQuery()) { return r.next(); }
        }
    }

    private boolean promoteIfCurrent(String alias, UUID expectedModelId, UUID candidateModelId,
                                     String metric, double previousScore, double candidateScore,
                                     double improvement) throws SQLException {
        if (!alias.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}"))
            throw new IllegalArgumentException("Invalid promotion alias");
        try (Connection c = connection()) {
            c.setAutoCommit(false);
            try (PreparedStatement current = c.prepareStatement(
                    "SELECT model_id FROM model_alias WHERE alias=? FOR UPDATE")) {
                current.setString(1, alias);
                try (ResultSet r = current.executeQuery()) {
                    if (!r.next() || !expectedModelId.equals(r.getObject(1, UUID.class))) {
                        c.rollback();
                        return false;
                    }
                }
            }
            try (PreparedStatement ready = c.prepareStatement(
                    "SELECT 1 FROM model_registry WHERE id=? AND status='READY' AND metric=?")) {
                ready.setObject(1, candidateModelId);
                ready.setString(2, metric);
                try (ResultSet r = ready.executeQuery()) {
                    if (!r.next()) throw new IllegalStateException("Candidate model is not READY with the expected metric");
                }
            }
            try (PreparedStatement update = c.prepareStatement(
                    "UPDATE model_alias SET model_id=?,updated_at=now() WHERE alias=?")) {
                update.setObject(1, candidateModelId);
                update.setString(2, alias);
                update.executeUpdate();
            }
            try (PreparedStatement event = c.prepareStatement(
                    "INSERT INTO model_promotion_event(id,alias,previous_model_id,promoted_model_id,metric,previous_score,candidate_score,improvement,reason,status) "
                            + "VALUES(?,?,?,?,?,?,?,?,?,'PROMOTED')")) {
                event.setObject(1, UUID.randomUUID());
                event.setString(2, alias);
                event.setObject(3, expectedModelId);
                event.setObject(4, candidateModelId);
                event.setString(5, metric);
                event.setDouble(6, previousScore);
                event.setDouble(7, candidateScore);
                event.setDouble(8, improvement);
                event.setString(9, "DRIFT_TRIGGERED_AUTO_RETRAIN");
                event.executeUpdate();
            }
            c.commit();
            return true;
        }
    }

    public Map<String, Object> rollbackAlias(String alias) throws SQLException {
        try (Connection c = connection()) {
            c.setAutoCommit(false);
            UUID eventId;
            UUID previousModelId;
            UUID promotedModelId;
            try (PreparedStatement event = c.prepareStatement(
                    "SELECT id,previous_model_id,promoted_model_id FROM model_promotion_event "
                            + "WHERE alias=? AND status='PROMOTED' ORDER BY created_at DESC LIMIT 1 FOR UPDATE")) {
                event.setString(1, alias);
                try (ResultSet r = event.executeQuery()) {
                    if (!r.next()) throw new NoSuchElementException("No promotion is available to roll back");
                    eventId = r.getObject(1, UUID.class);
                    previousModelId = r.getObject(2, UUID.class);
                    promotedModelId = r.getObject(3, UUID.class);
                }
            }
            try (PreparedStatement current = c.prepareStatement(
                    "SELECT model_id FROM model_alias WHERE alias=? FOR UPDATE")) {
                current.setString(1, alias);
                try (ResultSet r = current.executeQuery()) {
                    if (!r.next() || !promotedModelId.equals(r.getObject(1, UUID.class)))
                        throw new IllegalStateException("Alias has changed since the recorded promotion");
                }
            }
            try (PreparedStatement ready = c.prepareStatement(
                    "SELECT 1 FROM model_registry WHERE id=? AND status='READY'")) {
                ready.setObject(1, previousModelId);
                try (ResultSet r = ready.executeQuery()) {
                    if (!r.next()) throw new IllegalStateException("Previous model is not READY; rollback is unsafe");
                }
            }
            try (PreparedStatement update = c.prepareStatement(
                    "UPDATE model_alias SET model_id=?,updated_at=now() WHERE alias=?")) {
                update.setObject(1, previousModelId);
                update.setString(2, alias);
                update.executeUpdate();
            }
            try (PreparedStatement mark = c.prepareStatement(
                    "UPDATE model_promotion_event SET status='ROLLED_BACK',rolled_back_at=now() WHERE id=?")) {
                mark.setObject(1, eventId);
                mark.executeUpdate();
            }
            c.commit();
            return Map.of("alias", alias, "restoredModelId", previousModelId.toString(),
                    "rolledBackModelId", promotedModelId.toString(), "status", "ROLLED_BACK");
        }
    }

    public List<Map<String, Object>> monitoringHistory(UUID modelId, int limit) throws SQLException {
        model(modelId); // verifies the model exists
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "SELECT id,row_count,drift_level,report,created_at FROM model_monitoring_run WHERE model_id=? ORDER BY created_at DESC LIMIT ?")) {
            p.setObject(1, modelId);
            p.setInt(2, Math.max(1, Math.min(limit, 200)));
            try (ResultSet r = p.executeQuery()) {
                List<Map<String, Object>> result = new ArrayList<>();
                while (r.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("runId", r.getObject("id").toString());
                    row.put("rowCount", r.getInt("row_count"));
                    row.put("driftLevel", r.getString("drift_level"));
                    row.put("report", parseJson(r.getString("report")));
                    row.put("createdAt", r.getTimestamp("created_at").toInstant().toString());
                    result.add(row);
                }
                return result;
            }
        }
    }

    public Map<String, Object> experiment(UUID id) throws SQLException {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "SELECT id,name,task_type,label_column,feature_columns,status,best_algorithm,best_metric,best_score,model_uri,metadata,error_message,created_at,finished_at FROM automl_experiment WHERE id=?")) {
            p.setObject(1, id);
            try (ResultSet r = p.executeQuery()) {
                if (!r.next()) throw new NoSuchElementException("Experiment not found");
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("id", id.toString());
                result.put("name", r.getString("name"));
                result.put("task", r.getString("task_type"));
                result.put("label", r.getString("label_column"));
                result.put("features", r.getString("feature_columns"));
                result.put("status", r.getString("status"));
                result.put("algorithm", r.getString("best_algorithm"));
                result.put("metric", r.getString("best_metric"));
                result.put("score", r.getObject("best_score"));
                result.put("modelUri", r.getString("model_uri"));
                result.put("metadata", parseJson(r.getString("metadata")));
                result.put("error", r.getString("error_message"));
                result.put("createdAt", r.getTimestamp("created_at").toInstant().toString());
                Timestamp finished = r.getTimestamp("finished_at");
                result.put("finishedAt", finished == null ? null : finished.toInstant().toString());
                List<Map<String, Object>> trials = new ArrayList<>();
                try (PreparedStatement t = c.prepareStatement(
                        "SELECT trial_number,algorithm,parameters,metrics,status,duration_ms,error_message FROM automl_trial WHERE experiment_id=? ORDER BY trial_number")) {
                    t.setObject(1, id);
                    try (ResultSet tr = t.executeQuery()) {
                        while (tr.next()) {
                            Map<String, Object> trial = new LinkedHashMap<>();
                            trial.put("number", tr.getInt("trial_number"));
                            trial.put("algorithm", tr.getString("algorithm"));
                            trial.put("parameters", tr.getString("parameters"));
                            trial.put("metrics", tr.getString("metrics"));
                            trial.put("status", tr.getString("status"));
                            trial.put("durationMs", tr.getLong("duration_ms"));
                            trial.put("error", tr.getString("error_message"));
                            trials.add(trial);
                        }
                    }
                }
                result.put("trials", trials);
                return result;
            }
        }
    }

    public List<Map<String, Object>> models(int limit) throws SQLException {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "SELECT id,experiment_id,name,algorithm,task_type,metric,best_score,version,status,created_at FROM model_registry ORDER BY created_at DESC LIMIT ?")) {
            p.setInt(1, Math.max(1, Math.min(limit, 200)));
            try (ResultSet r = p.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (r.next()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", r.getObject("id").toString());
                    m.put("experimentId", r.getObject("experiment_id").toString());
                    m.put("name", r.getString("name"));
                    m.put("algorithm", r.getString("algorithm"));
                    m.put("task", r.getString("task_type"));
                    m.put("metric", r.getString("metric"));
                    m.put("score", r.getDouble("best_score"));
                    m.put("version", r.getInt("version"));
                    m.put("status", r.getString("status"));
                    m.put("createdAt", r.getTimestamp("created_at").toInstant().toString());
                    rows.add(m);
                }
                return rows;
            }
        }
    }

    public List<Map<String, Object>> aliases() throws SQLException {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "SELECT a.alias,a.model_id,m.name,m.algorithm,m.task_type,m.metric,m.best_score,m.status,a.updated_at "
                        + "FROM model_alias a JOIN model_registry m ON m.id=a.model_id ORDER BY a.alias")) {
            try (ResultSet r = p.executeQuery()) {
                List<Map<String, Object>> result = new ArrayList<>();
                while (r.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("alias", r.getString("alias"));
                    row.put("modelId", r.getObject("model_id").toString());
                    row.put("name", r.getString("name"));
                    row.put("algorithm", r.getString("algorithm"));
                    row.put("task", r.getString("task_type"));
                    row.put("metric", r.getString("metric"));
                    row.put("score", r.getObject("best_score"));
                    row.put("modelStatus", r.getString("status"));
                    row.put("updatedAt", r.getTimestamp("updated_at").toInstant().toString());
                    result.add(row);
                }
                return result;
            }
        }
    }

    public Map<String, Object> alias(String alias) throws SQLException {
        if (alias == null || !alias.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}"))
            throw new IllegalArgumentException("Invalid model alias");
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "SELECT a.alias,a.model_id,m.name,m.algorithm,m.task_type,m.metric,m.best_score,m.status,a.updated_at "
                        + "FROM model_alias a JOIN model_registry m ON m.id=a.model_id WHERE a.alias=?")) {
            p.setString(1, alias);
            try (ResultSet r = p.executeQuery()) {
                if (!r.next()) throw new NoSuchElementException("Model alias not found");
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("alias", r.getString("alias"));
                result.put("modelId", r.getObject("model_id").toString());
                result.put("name", r.getString("name"));
                result.put("algorithm", r.getString("algorithm"));
                result.put("task", r.getString("task_type"));
                result.put("metric", r.getString("metric"));
                result.put("score", r.getObject("best_score"));
                result.put("modelStatus", r.getString("status"));
                result.put("updatedAt", r.getTimestamp("updated_at").toInstant().toString());
                return result;
            }
        }
    }

    public void archive(UUID id) throws SQLException {
        try (Connection c = connection()) {
            c.setAutoCommit(false);
            try (PreparedStatement p = c.prepareStatement("UPDATE model_registry SET status='ARCHIVED' WHERE id=? AND status='READY'")) {
                p.setObject(1, id);
                if (p.executeUpdate() != 1) throw new NoSuchElementException("Ready model not found");
            }
            try (PreparedStatement p = c.prepareStatement("DELETE FROM model_alias WHERE model_id=?")) {
                p.setObject(1, id);
                p.executeUpdate();
            }
            c.commit();
        }
    }

    public void promote(String alias, UUID modelId) throws SQLException {
        if (alias == null || !alias.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}"))
            throw new IllegalArgumentException("Invalid model alias");
        try (Connection c = connection()) {
            c.setAutoCommit(false);
            try (PreparedStatement check = c.prepareStatement("SELECT 1 FROM model_registry WHERE id=? AND status='READY'")) {
                check.setObject(1, modelId);
                try (ResultSet r = check.executeQuery()) {
                    if (!r.next()) throw new NoSuchElementException("Ready model not found");
                }
            }
            try (PreparedStatement upsert = c.prepareStatement(
                    "INSERT INTO model_alias(alias,model_id) VALUES(?,?) ON CONFLICT(alias) DO UPDATE SET model_id=EXCLUDED.model_id,updated_at=now()")) {
                upsert.setString(1, alias);
                upsert.setObject(2, modelId);
                upsert.executeUpdate();
            }
            c.commit();
        }
    }

    private void completeTraining(UUID experimentId, UUID modelId, String name, String algorithm, String task,
                                  String metric, double score, String modelPath, List<?> features,
                                  List<Map<String, Object>> trials, Map<String, Object> modelCard) throws Exception {
        String cardJson = JSON.writeValueAsString(modelCard);
        try (Connection c = connection()) {
            c.setAutoCommit(false);
            try (PreparedStatement p = c.prepareStatement(
                    "INSERT INTO automl_trial(experiment_id,trial_number,algorithm,parameters,metrics,status,duration_ms,error_message) VALUES(?,?,?,?::jsonb,?::jsonb,?,?,?)")) {
                int index = 0;
                for (Map<String, Object> t : trials) {
                    p.setObject(1, experimentId);
                    p.setInt(2, ++index);
                    p.setString(3, String.valueOf(t.get("algorithm")));
                    p.setString(4, "{}");
                    p.setString(5, JSON.writeValueAsString(t));
                    p.setString(6, String.valueOf(t.get("status")));
                    p.setLong(7, ((Number) t.getOrDefault("durationMs", 0)).longValue());
                    p.setString(8, t.get("error") == null ? null : String.valueOf(t.get("error")));
                    p.addBatch();
                }
                p.executeBatch();
            }
            try (PreparedStatement p = c.prepareStatement(
                    "INSERT INTO model_registry(id,experiment_id,name,algorithm,task_type,metric,best_score,model_uri,version,status,feature_columns,model_card) VALUES(?,?,?,?,?,?,?,?,1,'READY',?::jsonb,?::jsonb)")) {
                p.setObject(1, modelId);
                p.setObject(2, experimentId);
                p.setString(3, name);
                p.setString(4, algorithm);
                p.setString(5, task);
                p.setString(6, metric);
                p.setDouble(7, score);
                p.setString(8, modelPath);
                p.setString(9, JSON.writeValueAsString(features));
                p.setString(10, cardJson);
                p.executeUpdate();
            }
            try (PreparedStatement p = c.prepareStatement(
                    "UPDATE automl_experiment SET status='SUCCEEDED',best_algorithm=?,best_metric=?,best_score=?,model_uri=?,metadata=?::jsonb,finished_at=now() WHERE id=? AND status='RUNNING'")) {
                p.setString(1, algorithm);
                p.setString(2, metric);
                p.setDouble(3, score);
                p.setString(4, modelPath);
                p.setString(5, cardJson);
                p.setObject(6, experimentId);
                if (p.executeUpdate() != 1) throw new SQLException("Experiment was not RUNNING at completion");
            }
            c.commit();
        }
    }

    private void insertExperiment(UUID id, String name, String task, String label, String features) throws SQLException {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "INSERT INTO automl_experiment(id,name,task_type,label_column,feature_columns,status) VALUES(?,?,?,?,?::jsonb,'QUEUED')")) {
            p.setObject(1, id);
            p.setString(2, name);
            p.setString(3, task);
            p.setString(4, label);
            p.setString(5, features);
            p.executeUpdate();
        }
    }

    private void updateStatus(UUID id, String status, String algorithm, String metric, Double score, String uri) throws SQLException {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "UPDATE automl_experiment SET status=?,best_algorithm=?,best_metric=?,best_score=?,model_uri=?,finished_at=CASE WHEN ? IN ('SUCCEEDED','FAILED','CANCELLED') THEN now() ELSE finished_at END WHERE id=?")) {
            p.setString(1, status);
            p.setString(2, algorithm);
            p.setString(3, metric);
            if (score == null) p.setNull(4, Types.DOUBLE);
            else p.setDouble(4, score);
            p.setString(5, uri);
            p.setString(6, status);
            p.setObject(7, id);
            p.executeUpdate();
        }
    }

    private void updateFailure(UUID id, String message) throws SQLException {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement(
                "UPDATE automl_experiment SET status='FAILED',error_message=?,finished_at=now() WHERE id=?")) {
            p.setString(1, message == null ? "Training failed" : message.substring(0, Math.min(message.length(), 4000)));
            p.setObject(2, id);
            p.executeUpdate();
        }
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    private static String jsonString(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (Exception error) {
            throw new IllegalArgumentException("Could not encode model metadata", error);
        }
    }

    private static Map<String, Object> parseJson(String value) throws SQLException {
        try {
            return JSON.readValue(value == null ? "{}" : value, new TypeReference<Map<String, Object>>() {});
        } catch (Exception error) {
            throw new SQLException("Could not decode persisted model metadata", error);
        }
    }

    private static String env(String key, String fallback) {
        return Optional.ofNullable(System.getenv(key)).filter(s -> !s.isBlank()).orElse(fallback);
    }
}
