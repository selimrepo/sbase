package com.sbase.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.doris.DorisClient;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
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
    private final PipelineRepository pipelines;
    private final PipelineRunRepository runs;
    private final PipelineDefinitionValidator validator;
    private final ObjectMapper mapper;
    private final ObjectProvider<SparkSession> sparkProvider;
    private final DorisClient doris;
    private final Executor executor;
    private final Set<String> allowedTables;

    public PipelineExecutionService(PipelineRepository pipelines, PipelineRunRepository runs,
            PipelineDefinitionValidator validator, ObjectMapper mapper, ObjectProvider<SparkSession> sparkProvider,
            DorisClient doris, @Qualifier("miningExecutor") Executor executor,
            @Value("${sbase.doris.allowed-tables:}") String allowedTables) {
        this.pipelines = pipelines; this.runs = runs; this.validator = validator; this.mapper = mapper;
        this.sparkProvider = sparkProvider; this.doris = doris; this.executor = executor;
        this.allowedTables = new HashSet<>();
        for (String table : allowedTables.split(",")) if (!table.isBlank()) this.allowedTables.add(table.trim().toLowerCase(Locale.ROOT));
    }

    @Transactional
    public PipelineRunEntity submit(UUID pipelineId) {
        PipelineEntity pipeline = pipelines.findById(pipelineId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline not found"));
        if (!pipeline.isEnabled()) throw new ResponseStatusException(HttpStatus.CONFLICT, "Pipeline is disabled");
        validator.parseAndValidate(pipeline.getDefinitionJson());
        PipelineRunEntity run = runs.save(new PipelineRunEntity(pipelineId, pipeline.getVersion(), pipeline.getDefinitionJson()));
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
                JsonNode step = steps.get(id);
                String type = step.path("type").asText();
                List<String> dependencies = new ArrayList<>();
                step.path("dependsOn").forEach(d -> dependencies.add(d.asText()));
                Dataset<Row> input = dependencies.isEmpty() ? current : outputs.get(dependencies.get(0));
                Dataset<Row> result = apply(type, step, input);
                if (result != null) {
                    outputs.put(id, result);
                    current = result;
                    if (!"doris_write".equals(type)) processed = result.count();
                }
                stepResults.add(Map.of("stepId", id, "type", type, "status", "SUCCEEDED"));
            }
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("steps", stepResults);
            summary.put("recordsProcessed", processed);
            summary.put("pipelineVersion", run.getPipelineVersion());
            markSucceeded(runId, mapper.writeValueAsString(summary), processed);
        } catch (Exception ex) {
            markFailed(runId, rootMessage(ex));
        }
    }

    private Dataset<Row> apply(String type, JsonNode step, Dataset<Row> input) {
        return switch (type) {
            case "doris_read" -> readDoris(step.path("table").asText());
            case "filter" -> filter(input, step);
            case "select" -> select(input, step);
            case "with_column" -> withColumn(input, step);
            case "aggregate" -> aggregate(input, step);
            case "doris_write" -> { writeDoris(requireInput(input), step); yield input; }
            default -> throw new IllegalArgumentException("Unsupported pipeline step: " + type);
        };
    }

    private Dataset<Row> readDoris(String table) {
        requireAllowed(table);
        return sparkProvider.getObject().read().format("jdbc").option("url", doris.jdbcUrl()).option("dbtable", table)
                .option("user", doris.username()).option("password", doris.password())
                .option("driver", "com.mysql.cj.jdbc.Driver").load();
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

    private void writeDoris(Dataset<Row> df, JsonNode step) {
        String table = step.path("table").asText();
        requireAllowed(table);
        String mode = step.path("mode").asText("overwrite");
        df.write().format("jdbc").option("url", doris.jdbcUrl()).option("dbtable", table)
                .option("user", doris.username()).option("password", doris.password())
                .option("driver", "com.mysql.cj.jdbc.Driver").option("batchsize", "5000")
                .mode(mode).save();
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
        PipelineRunEntity run = runs.findById(id).orElse(null);
        if (run == null || !"QUEUED".equals(run.getStatus())) return null;
        run.start(); return runs.save(run);
    }
    private void markSucceeded(UUID id, String result, long records) {
        runs.findById(id).ifPresent(run -> { if ("RUNNING".equals(run.getStatus())) { run.succeed(result, records); runs.save(run); } });
    }
    private void markFailed(UUID id, String error) {
        runs.findById(id).ifPresent(run -> { if ("QUEUED".equals(run.getStatus()) || "RUNNING".equals(run.getStatus())) { run.fail(error); runs.save(run); } });
    }
    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }
}
