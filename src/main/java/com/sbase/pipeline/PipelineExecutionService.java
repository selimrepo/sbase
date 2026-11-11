package com.sbase.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.doris.DorisClient;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.Window;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import java.time.Instant;
import java.time.Duration;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;
import java.util.concurrent.Executor;

import static org.apache.spark.sql.functions.*;

@Service
public class PipelineExecutionService {
    private final JdbcTemplate jdbc;
    private final PipelineRepository pipelines;
    private final PipelineRunRepository runs;
    private final PipelineDefinitionValidator validator;
    private final ObjectMapper mapper;
    private final ObjectProvider<SparkSession> sparkProvider;
    private final DorisClient doris;
    private final Executor executor;
    private final Set<String> allowedTables;
    private final int configuredMaxAttempts;
    private final String postgresUrl;
    private final String postgresUser;
    private final String postgresPassword;

    public PipelineExecutionService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, PipelineRepository pipelines, PipelineRunRepository runs,
            PipelineDefinitionValidator validator, ObjectMapper mapper, ObjectProvider<SparkSession> sparkProvider,
            DorisClient doris, @Qualifier("miningExecutor") Executor executor,
            @Value("${sbase.doris.allowed-tables:}") String allowedTables,
            @Value("${sbase.pipeline.retry.max-attempts:4}") int configuredMaxAttempts,
            @Value("${spring.datasource.url}") String postgresUrl,
            @Value("${spring.datasource.username}") String postgresUser,
            @Value("${spring.datasource.password}") String postgresPassword) {
        this.jdbc = jdbc; this.pipelines = pipelines; this.runs = runs; this.validator = validator; this.mapper = mapper;
        this.sparkProvider = sparkProvider; this.doris = doris; this.executor = executor;
        this.configuredMaxAttempts = Math.max(1, Math.min(20, configuredMaxAttempts));
        this.postgresUrl = postgresUrl; this.postgresUser = postgresUser; this.postgresPassword = postgresPassword;
        this.allowedTables = new HashSet<>();
        for (String table : allowedTables.split(",")) if (!table.isBlank()) this.allowedTables.add(table.trim().toLowerCase(Locale.ROOT));
    }

    @Transactional
    public PipelineRunEntity submit(UUID pipelineId) {
        return submit(pipelineId, "system");
    }

    @Transactional
    public PipelineRunEntity submit(UUID pipelineId, String submittedBy) {
        PipelineEntity pipeline = pipelines.findById(pipelineId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline not found"));
        if (!pipeline.isEnabled()) throw new ResponseStatusException(HttpStatus.CONFLICT, "Pipeline is disabled");
        validator.parseAndValidate(pipeline.getDefinitionJson());
        PipelineRunEntity run = new PipelineRunEntity(pipelineId, pipeline.getVersion(), pipeline.getDefinitionJson(), submittedBy);
        run.setMaxAttempts(configuredMaxAttempts);
        run.setIdempotencyKey(UUID.randomUUID().toString());
        run = runs.save(run);
        UUID runId = run.getId();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { executor.execute(() -> execute(runId)); }
                catch (RuntimeException ex) { markFailed(runId, "Execution queue rejected the run: " + ex.getMessage()); }
            }
        });
        return run;
    }

    @Transactional(readOnly = true)
    public PipelineRunEntity getRun(UUID id) {
        return runs.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline run not found"));
    }

    @Transactional(readOnly = true)
    public List<PipelineRunEntity> listRuns(UUID pipelineId) {
        if (!pipelines.existsById(pipelineId)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline not found");
        return runs.findTop100ByPipelineIdOrderBySubmittedAtDesc(pipelineId);
    }

    public void execute(UUID runId) {
        PipelineRunEntity run = markRunning(runId);
        if (run == null) return;
        executeClaimed(runId, run);
    }

    private void executeClaimed(UUID runId, PipelineRunEntity run) {
        String currentStepId = null;
        long stepStartedNanos = 0L;
        var spark = sparkProvider.getObject();
        String jobGroup = "pipeline-run-" + runId;
        spark.sparkContext().setJobGroup(jobGroup, "SBase pipeline run " + runId, true);
        try {
            JsonNode definition = validator.parseAndValidate(run.getDefinitionSnapshot());
            Map<String, Dataset<Row>> outputs = new HashMap<>();
            Map<String, JsonNode> steps = new LinkedHashMap<>();
            definition.path("steps").forEach(step -> steps.put(step.path("id").asText(), step));
            List<String> ordered = topologicalOrder(steps);
            Dataset<Row> current = null;
            long processed = 0;
            List<Map<String, Object>> stepResults = new ArrayList<>();
            for (String id : ordered) {
                if (runs.findById(runId).map(r -> "CANCELLED".equals(r.getStatus())).orElse(true)) {
                    spark.sparkContext().cancelJobGroup(jobGroup);
                    return;
                }
                JsonNode step = steps.get(id);
                String type = step.path("type").asText();
                currentStepId = id;
                stepStartedNanos = System.nanoTime();
                jdbc.update("""
                    INSERT INTO pipeline_step_runs(id,pipeline_run_id,step_id,step_type,status,started_at,attempt)
                    VALUES(?,?,?,?,'RUNNING',now(),?)
                    ON CONFLICT(pipeline_run_id,step_id,attempt) DO UPDATE
                       SET status='RUNNING',started_at=now(),finished_at=NULL,duration_ms=NULL,
                           records_processed=0,output_schema_json=NULL,error_message=NULL
                    """, UUID.randomUUID(), runId, id, type, run.getAttemptCount());
                List<String> dependencies = new ArrayList<>();
                step.path("dependsOn").forEach(d -> dependencies.add(d.asText()));
                List<Dataset<Row>> inputs = dependencies.stream().map(outputs::get).toList();
                Dataset<Row> input = inputs.isEmpty() ? current : inputs.get(0);
                Dataset<Row> result = apply(type, step, input, inputs, runId, run.getIdempotencyKey());
                long stepRecords = 0;
                String schema = null;
                if (result != null) {
                    outputs.put(id, result);
                    current = result;
                    schema = mapper.writeValueAsString(Arrays.asList(result.columns()));
                    if (!"doris_write".equals(type)) {
                        stepRecords = result.count();
                        processed = stepRecords;
                    }
                }
                long duration = Duration.ofNanos(System.nanoTime() - stepStartedNanos).toMillis();
                jdbc.update("""
                    UPDATE pipeline_step_runs
                       SET status='SUCCEEDED',finished_at=now(),duration_ms=?,
                           records_processed=?,output_schema_json=?
                     WHERE pipeline_run_id=? AND step_id=? AND attempt=?
                    """, duration, stepRecords, schema, runId, id, run.getAttemptCount());
                stepResults.add(Map.of("stepId", id, "type", type, "status", "SUCCEEDED",
                    "durationMs", duration, "recordsProcessed", stepRecords));
                currentStepId = null;
            }
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("steps", stepResults);
            summary.put("recordsProcessed", processed);
            summary.put("pipelineVersion", run.getPipelineVersion());
            summary.put("idempotencyKey", run.getIdempotencyKey());
            markSucceeded(runId, mapper.writeValueAsString(summary), processed);
        } catch (Exception ex) {
            String failure = rootMessage(ex);
            if (currentStepId != null) {
                boolean cancelled = runs.findById(runId).map(r -> "CANCELLED".equals(r.getStatus())).orElse(false);
                jdbc.update("""
                    UPDATE pipeline_step_runs
                       SET status=?,finished_at=now(),duration_ms=?,error_message=?
                     WHERE pipeline_run_id=? AND step_id=? AND attempt=?
                    """, cancelled ? "CANCELLED" : "FAILED",
                    Duration.ofNanos(Math.max(0L, System.nanoTime() - stepStartedNanos)).toMillis(),
                    failure, runId, currentStepId, run.getAttemptCount());
            }
            markFailed(runId, failure, isRetryable(ex));
        } finally {
            spark.sparkContext().clearJobGroup();
        }
    }

    @Transactional
    public PipelineRunEntity cancel(UUID runId) {
        PipelineRunEntity run = runs.findById(runId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline run not found"));
        if (Set.of("SUCCEEDED", "FAILED", "CANCELLED").contains(run.getStatus()))
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Pipeline run is already terminal");
        run.cancel();
        PipelineRunEntity saved = runs.save(run);
        var spark = sparkProvider.getIfAvailable();
        if (spark != null) spark.sparkContext().cancelJobGroup("pipeline-run-" + runId);
        return saved;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> stepRuns(UUID runId) {
        if (!runs.existsById(runId))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline run not found");
        return jdbc.queryForList("""
            SELECT step_id,step_type,status,started_at,finished_at,duration_ms,
                   records_processed,output_schema_json,error_message,attempt
              FROM pipeline_step_runs
             WHERE pipeline_run_id=?
             ORDER BY started_at,step_id
            """, runId);
    }

    private Dataset<Row> apply(String type, JsonNode step, Dataset<Row> input, List<Dataset<Row>> inputs, UUID runId, String idempotencyKey) {
        return switch (type) {
            case "doris_read" -> readDoris(step.path("table").asText());
            case "filter" -> filter(input, step);
            case "select" -> select(input, step);
            case "with_column" -> withColumn(input, step);
            case "aggregate" -> aggregate(input, step);
            case "join" -> join(inputs, step);
            case "union" -> union(inputs, step);
            case "feature_store_write" -> { requireFeatureWrite(step.path("featureSet").asText(), runId); writeOnlineFeatures(requireInput(input), step, runId); yield input; }
            case "doris_write" -> { writeDoris(requireInput(input), step, runId, idempotencyKey); yield input; }
            default -> throw new IllegalArgumentException("Unsupported pipeline step: " + type);
        };
    }

    private record FeatureOwner(UUID id, String lifecycle, String owner) { }\n\n    public boolean pipelineWritesFeatures(UUID pipelineId) {
        PipelineEntity pipeline = pipelines.findById(pipelineId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline not found"));
        JsonNode definition = validator.parseAndValidate(pipeline.getDefinitionJson());
        for (JsonNode step : definition.path("steps"))
            if ("feature_store_write".equals(step.path("type").asText())) return true;
        return false;
    }

    private void requireFeatureWrite(String featureSet, UUID runId) {
        PipelineRunEntity run = runs.findById(runId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline run not found"));
        FeatureOwner feature = jdbc.query("""
            SELECT id, lifecycle_state, owner_subject FROM feature_sets WHERE name=?
            """, rs -> rs.next() ? new FeatureOwner(rs.getObject("id", UUID.class),
                rs.getString("lifecycle_state"), rs.getString("owner_subject")) : null, featureSet);
        if (feature == null) throw new IllegalArgumentException("Unknown feature set: " + featureSet);
        if (!"ACTIVE".equalsIgnoreCase(feature.lifecycle()))
            throw new org.springframework.security.access.AccessDeniedException("Feature set is not active for materialization");
        if (run.getSubmittedBy().equals(feature.owner())) return;
        Integer grantCount = jdbc.queryForObject("SELECT count(*) FROM feature_access_grants WHERE feature_set_id=?", Integer.class, feature.id());
        if ((grantCount == null || grantCount == 0) && feature.owner() == null) return;
        if (grantCount == null || grantCount == 0)
            throw new org.springframework.security.access.AccessDeniedException("Feature-set owner has not granted pipeline access");
        String grant = jdbc.query("""
            SELECT permission FROM feature_access_grants WHERE feature_set_id=? AND subject=?
            """, rs -> rs.next() ? rs.getString(1) : null, feature.id(), run.getSubmittedBy());
        if (!"ADMIN".equals(grant) && !"WRITE".equals(grant))
            throw new org.springframework.security.access.AccessDeniedException("Pipeline submitter has no WRITE grant for feature set " + featureSet);
    }

    private Dataset<Row> readDoris(String table) {
        requireAllowed(table);
        return sparkProvider.getObject().read().format("jdbc").option("url", doris.jdbcUrl()).option("dbtable", table)
                .option("user", doris.username()).option("password", doris.password())
                .option("driver", "com.mysql.cj.jdbc.Driver").load();
    }


    private Dataset<Row> join(List<Dataset<Row>> inputs, JsonNode step) {
        if (inputs == null || inputs.size() != 2 || inputs.get(0) == null || inputs.get(1) == null)
            throw new IllegalArgumentException("Join requires two materialized input datasets");
        Dataset<Row> left = inputs.get(0).alias("left_source");
        Dataset<Row> right = inputs.get(1).alias("right_source");
        String leftKey = step.path("leftKey").asText();
        String rightKey = step.path("rightKey").asText();
        String joinType = switch (step.path("joinType").asText()) {
            case "inner" -> "inner";
            case "left" -> "left_outer";
            case "right" -> "right_outer";
            case "full" -> "full_outer";
            default -> throw new IllegalArgumentException("Unsupported join type");
        };
        Column condition = col("left_source." + leftKey).equalTo(col("right_source." + rightKey));
        Set<String> leftNames = new HashSet<>(Arrays.asList(left.columns()));
        List<Column> projection = new ArrayList<>();
        for (String name : left.columns()) projection.add(col("left_source." + name).alias(name));
        for (String name : right.columns())
            projection.add(col("right_source." + name).alias(leftNames.contains(name) ? "right_" + name : name));
        return left.join(right, condition, joinType).select(projection.toArray(Column[]::new));
    }


    private Dataset<Row> union(List<Dataset<Row>> inputs, JsonNode step) {
        if (inputs == null || inputs.size() != 2 || inputs.get(0) == null || inputs.get(1) == null)
            throw new IllegalArgumentException("Union requires two materialized input datasets");
        Dataset<Row> merged = inputs.get(0).unionByName(inputs.get(1), true);
        return "distinct".equals(step.path("mode").asText("all")) ? merged.dropDuplicates() : merged;
    }

    private Dataset<Row> filter(Dataset<Row> input, JsonNode step) {
        Dataset<Row> df = requireInput(input);
        Column column = col(step.path("column").asText());
        String op = step.path("operator").asText();
        JsonNode value = step.get("value");
        return switch (op) {
            case "eq" -> df.filter(column.equalTo(literal(value)));
            case "ne" -> df.filter(column.notEqual(literal(value)));
            case "gt" -> df.filter(column.gt(literal(value)));
            case "gte" -> df.filter(column.geq(literal(value)));
            case "lt" -> df.filter(column.lt(literal(value)));
            case "lte" -> df.filter(column.leq(literal(value)));
            case "contains" -> df.filter(column.contains(value.asText()));
            case "is_null" -> df.filter(column.isNull());
            case "not_null" -> df.filter(column.isNotNull());
            case "in" -> {
                List<Object> values = new ArrayList<>();
                value.forEach(v -> values.add(literal(v)));
                yield df.filter(column.isInCollection(values));
            }
            default -> throw new IllegalArgumentException("Unsupported filter operator: " + op);
        };
    }

    private Dataset<Row> select(Dataset<Row> input, JsonNode step) {
        requireInput(input);
        Column[] columns = new Column[step.path("columns").size()];
        for (int i = 0; i < columns.length; i++) columns[i] = col(step.path("columns").get(i).asText());
        return input.select(columns);
    }

    private Dataset<Row> withColumn(Dataset<Row> input, JsonNode step) {
        Dataset<Row> df = requireInput(input);
        Column source = col(step.path("sourceColumn").asText());
        Column transformed = switch (step.path("operation").asText()) {
            case "cast_double" -> source.cast(DataTypes.DoubleType);
            case "lower" -> lower(source);
            case "upper" -> upper(source);
            case "trim" -> trim(source);
            case "abs" -> abs(source);
            case "log1p" -> log1p(source.cast(DataTypes.DoubleType));
            default -> throw new IllegalArgumentException("Unsupported column operation");
        };
        return df.withColumn(step.path("column").asText(), transformed);
    }

    private Dataset<Row> aggregate(Dataset<Row> input, JsonNode step) {
        Dataset<Row> df = requireInput(input);
        List<Column> groups = new ArrayList<>();
        step.path("groupBy").forEach(g -> groups.add(col(g.asText())));
        List<Column> metrics = new ArrayList<>();
        for (JsonNode metric : step.path("metrics")) {
            Column source = col(metric.path("column").asText());
            Column value = switch (metric.path("function").asText()) {
                case "count" -> count(source);
                case "sum" -> sum(source);
                case "avg" -> avg(source);
                case "min" -> min(source);
                case "max" -> max(source);
                default -> throw new IllegalArgumentException("Unsupported aggregate function");
            };
            metrics.add(value.alias(metric.path("alias").asText()));
        }
        return df.groupBy(groups.toArray(Column[]::new)).agg(metrics.get(0), metrics.subList(1, metrics.size()).toArray(Column[]::new));
    }

    private void writeDoris(Dataset<Row> df, JsonNode step, UUID runId, String idempotencyKey) {
        String table = step.path("table").asText();
        requireAllowed(table);
        String mode = step.path("mode").asText("append");
        JsonNode keys = step.path("idempotencyColumns");
        if (!keys.isArray() || keys.isEmpty())
            throw new IllegalArgumentException("Doris writes require idempotencyColumns; configure the target as a Doris UNIQUE KEY table using these columns");
        String[] keyColumns = new String[keys.size()];
        for (int i = 0; i < keys.size(); i++) { keyColumns[i] = keys.get(i).asText(); PipelineDefinitionValidator.identifier(keyColumns[i]); }
        verifyDorisUniqueKey(table, keyColumns);
        Dataset<Row> stable = df.dropDuplicates(keyColumns);
        // The run-level Spark job group is set by executeClaimed, allowing
        // cancellation to interrupt this external write as well.
        stable.write().format("jdbc").option("url", doris.jdbcUrl()).option("dbtable", table)
                .option("user", doris.username()).option("password", doris.password())
                .option("driver", "com.mysql.cj.jdbc.Driver").option("batchsize", "5000")
                .mode("append").save();
    }



    private void writeOnlineFeatures(Dataset<Row> source, JsonNode step, UUID runId) {
        String featureSet = step.path("featureSet").asText();
        String entityKeyColumn = step.path("entityKeyColumn").asText();
        String eventTimeColumn = step.path("eventTimeColumn").asText("");
        int featureVersion = step.path("featureVersion").asInt(1);
        if (!Arrays.asList(source.columns()).contains(entityKeyColumn))
            throw new IllegalArgumentException("Online feature entity key column is missing: " + entityKeyColumn);
        if (!eventTimeColumn.isBlank() && !Arrays.asList(source.columns()).contains(eventTimeColumn))
            throw new IllegalArgumentException("Online feature event-time column is missing: " + eventTimeColumn);
        String[] featureColumns = Arrays.stream(source.columns())
            .filter(name -> !name.equals(entityKeyColumn) && !name.equals(eventTimeColumn))
            .toArray(String[]::new);
        if (featureColumns.length == 0) throw new IllegalArgumentException("Online feature materialization requires at least one feature column");

        Dataset<Row> latest = source;
        if (!eventTimeColumn.isBlank()) {
            latest = source.withColumn("__sbase_rn", row_number().over(
                Window.partitionBy(col(entityKeyColumn)).orderBy(col(eventTimeColumn).desc_nulls_last())))
                .filter(col("__sbase_rn").equalTo(1)).drop("__sbase_rn");
        } else {
            latest = source.dropDuplicates(entityKeyColumn);
        }
        Column[] featureExpressions = Arrays.stream(featureColumns).map(org.apache.spark.sql.functions::col).toArray(Column[]::new);
        Column eventTime = eventTimeColumn.isBlank() ? lit(null).cast(DataTypes.TimestampType) : col(eventTimeColumn).cast(DataTypes.TimestampType);
        Dataset<Row> staged = latest.select(
            lit(featureSet).alias("feature_set"),
            col(entityKeyColumn).cast(DataTypes.StringType).alias("entity_key"),
            lit(featureVersion).alias("feature_version"),
            to_json(struct(featureExpressions)).alias("feature_values"),
            eventTime.alias("event_time")
        ).repartition(8);

        String stageTable = "sbase_feature_stage_" + UUID.nameUUIDFromBytes(
            (runId + ":" + step.path("id").asText()).getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString().replace("-", "");
        String stageName = stageTable;
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);
             java.sql.Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + stageName + " (" +
                "feature_set VARCHAR(160) NOT NULL, entity_key VARCHAR(512) NOT NULL, " +
                "feature_version INTEGER NOT NULL, feature_values TEXT NOT NULL, event_time TIMESTAMPTZ)");
            statement.executeUpdate("TRUNCATE TABLE " + stageName);
        } catch (java.sql.SQLException ex) {
            throw new IllegalStateException("Cannot prepare online feature staging table", ex);
        }

        try {
            staged.write().format("jdbc").option("url", postgresUrl).option("dbtable", stageName)
                .option("user", postgresUser).option("password", postgresPassword)
                .option("driver", "org.postgresql.Driver").option("batchsize", "2000")
                .option("numPartitions", "8").mode("append").save();

            try (java.sql.Connection connection = java.sql.DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);
                 java.sql.Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                statement.executeUpdate("INSERT INTO online_feature_values(feature_set, entity_key, feature_version, feature_values, event_time, materialized_at) " +
                    "SELECT feature_set, entity_key, feature_version, feature_values::jsonb, COALESCE(event_time, now()), now() FROM " + stageName +
                    " ON CONFLICT(feature_set, entity_key) DO UPDATE SET feature_version=EXCLUDED.feature_version, " +
                    "feature_values=EXCLUDED.feature_values, event_time=EXCLUDED.event_time, materialized_at=now() " +
                    "WHERE EXCLUDED.event_time >= online_feature_values.event_time");
                statement.execute("DROP TABLE " + stageName);
                connection.commit();
            } catch (java.sql.SQLException ex) {
                throw new IllegalStateException("Online feature materialization failed; retry is safe", ex);
            }
        } catch (RuntimeException ex) {
            try (java.sql.Connection connection = java.sql.DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);
                 java.sql.Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + stageName);
            } catch (java.sql.SQLException cleanupError) {
                ex.addSuppressed(cleanupError);
            }
            throw ex;
        }
    }

    private void verifyDorisUniqueKey(String table, String[] keyColumns) {
        try (java.sql.Connection connection = doris.openConnection();
             java.sql.Statement statement = connection.createStatement();
             java.sql.ResultSet result = statement.executeQuery("SHOW CREATE TABLE " + table)) {
            if (!result.next()) throw new IllegalStateException("Doris target table does not exist: " + table);
            String ddl = result.getString(2).toUpperCase(Locale.ROOT).replace("`", "");
            int keyStart = ddl.indexOf("UNIQUE KEY");
            if (keyStart < 0) throw new IllegalArgumentException(
                "Idempotent append requires a Doris UNIQUE KEY target table: " + table);
            int open = ddl.indexOf('(', keyStart);
            int close = ddl.indexOf(')', open);
            if (open < 0 || close < 0) throw new IllegalStateException("Cannot inspect Doris UNIQUE KEY definition");
            Set<String> declaredKeys = Arrays.stream(ddl.substring(open + 1, close).split(","))
                .map(String::trim).collect(java.util.stream.Collectors.toSet());
            Set<String> requestedKeys = Arrays.stream(keyColumns)
                .map(key -> key.toUpperCase(Locale.ROOT)).collect(java.util.stream.Collectors.toSet());
            if (!declaredKeys.equals(requestedKeys))
                throw new IllegalArgumentException("idempotencyColumns must exactly match the Doris UNIQUE KEY columns: " + declaredKeys);
        } catch (java.sql.SQLException ex) {
            throw new IllegalStateException("Cannot verify Doris idempotency constraints for " + table, ex);
        }
    }

    private Object literal(JsonNode value) {
        if (value == null || value.isNull()) return null;
        if (value.isBoolean()) return value.booleanValue();
        if (value.isNumber()) return value.numberValue();
        return value.asText();
    }

    private void requireAllowed(String table) {
        PipelineDefinitionValidator.table(table);
        if (!allowedTables.contains(table.toLowerCase(Locale.ROOT)))
            throw new IllegalArgumentException("Doris table is not allow-listed: " + table);
    }
    private Dataset<Row> requireInput(Dataset<Row> input) {
        if (input == null) throw new IllegalArgumentException("Step requires an input dataset");
        return input;
    }
    private List<String> topologicalOrder(Map<String, JsonNode> steps) {
        List<String> ordered = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        for (String id : steps.keySet()) visit(id, steps, visited, ordered);
        return ordered;
    }
    private void visit(String id, Map<String, JsonNode> steps, Set<String> visited, List<String> ordered) {
        if (!visited.add(id)) return;
        steps.get(id).path("dependsOn").forEach(d -> visit(d.asText(), steps, visited, ordered));
        ordered.add(id);
    }

    private PipelineRunEntity markRunning(UUID id) {
        if (runs.claimQueued(id) != 1) return null;
        return runs.findById(id).orElse(null);
    }
    private void markSucceeded(UUID id, String result, long records) {
        runs.findById(id).ifPresent(run -> { if ("RUNNING".equals(run.getStatus())) { run.succeed(result, records); runs.save(run); } });
    }
    private void markFailed(UUID id, String error) {
        markFailed(id, error, true);
    }

    private void markFailed(UUID id, String error, boolean retryable) {
        runs.findById(id).ifPresent(run -> {
            if ("QUEUED".equals(run.getStatus()) || "RUNNING".equals(run.getStatus())) {
                if (retryable) run.scheduleRetry(error, Instant.now().plus(retryDelay(run.getAttemptCount())));
                else run.fail(error);
                runs.save(run);
            }
        });
    }

    static boolean isRetryable(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof java.sql.SQLTransientException
                    || cause instanceof java.sql.SQLRecoverableException
                    || cause instanceof java.io.IOException
                    || cause instanceof java.util.concurrent.TimeoutException
                    || cause instanceof java.util.concurrent.RejectedExecutionException)
                return true;
            if (cause instanceof java.sql.SQLException sql && sql.getSQLState() != null
                    && sql.getSQLState().length() >= 2
                    && Set.of("08", "40", "53", "57").contains(sql.getSQLState().substring(0, 2)))
                return true;
        }
        return false;
    }

    @Transactional
    public void dispatchQueued(UUID runId) {
        if (runs.claimQueued(runId) != 1) return;
        PipelineRunEntity claimed = runs.findById(runId).orElse(null);
        if (claimed == null) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { executor.execute(() -> executeClaimed(runId, claimed)); }
                catch (RuntimeException ex) { markFailed(runId, "Execution queue rejected recovered run: " + ex.getMessage()); }
            }
        });
    }

    @Transactional
    public void recoverExpired(UUID runId) {
        runs.recoverExpired(runId);
    }

    @Transactional
    public void retryDue(UUID runId) {
        if (runs.claimDueRetry(runId) != 1) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { executor.execute(() -> execute(runId)); }
                catch (RuntimeException ex) { markFailed(runId, "Retry executor rejected run: " + ex.getMessage()); }
            }
        });
    }

    private static Duration retryDelay(int attempt) {
        long seconds = Math.min(900L, 10L * (1L << Math.min(7, Math.max(0, attempt - 1))));
        return Duration.ofSeconds(seconds);
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }
}
