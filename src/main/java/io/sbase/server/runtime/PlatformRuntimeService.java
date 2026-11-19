package io.sbase.server.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

/** Runtime-facing adapters for the Spark and Flink components already bundled with SBase. */
public final class PlatformRuntimeService {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};
    private final SparkSession spark;
    private final String flinkUrl;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(4)).build();

    public PlatformRuntimeService(SparkSession spark) {
        this.spark = Objects.requireNonNull(spark);
        this.flinkUrl = Optional.ofNullable(System.getenv("FLINK_REST_URL"))
                .filter(value -> !value.isBlank()).orElse("http://localhost:8082").replaceAll("/+$", "");
    }

    public List<Map<String, Object>> components() {
        List<Map<String, Object>> values = new ArrayList<>();
        values.add(component("workflow-engine", "Workflow Engine", "Apache Pekko Typed/Cluster Sharding", "available",
                List.of("immutable-versions", "graph-validation", "durable-jobs", "leases", "retries", "schedules", "execution-events", "published-deployments", "rest-triggered-runs", "version-pinning", "concurrency-limits", "audit-trail")));
        values.add(component("catalog", "Data Catalog", "PostgreSQL, Cassandra and Solr adapters", "available",
                List.of("discovery", "read-only-query", "schema-inspection", "federated-sources")));
        values.add(component("query", "Query Engine", "Apache Calcite and Spark SQL", "available",
                List.of("sql-validation", "spark-sql", "federated-query-planning", "federated-query-execution", "predicate-pushdown")));
        values.add(component("automl", "Scientific AutoML", "Spark ML", "available",
                List.of("classification", "regression", "time-series", "cross-validation", "fairness", "model-cards", "async-jobs", "drift-monitoring")));
        values.add(component("mining", "Scientific Mining", "Apache Mahout", "available",
                List.of("vector-sum", "cosine-similarity", "euclidean-distance")));
        values.add(component("stream-processing", "Stream Processing", "Apache Flink REST adapter", "configured",
                List.of("jar-submit", "jobs-overview", "job-details", "taskmanagers", "jars")));
        values.add(component("model-registry", "Model Registry", "PostgreSQL metadata and model artifacts", "available",
                List.of("registry", "model-cards", "prediction", "aliases", "promotion", "rollback", "archive")));
        return values;
    }

    private static Map<String, Object> component(String id, String name, String technology,
                                                  String availability, List<String> features) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", id); value.put("name", name); value.put("technology", technology);
        value.put("availability", availability); value.put("features", features);
        return value;
    }

    public Map<String, Object> engines() {
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Object> sparkInfo = new LinkedHashMap<>();
        boolean stopped = spark.sparkContext().isStopped();
        sparkInfo.put("status", stopped ? "STOPPED" : "UP");
        sparkInfo.put("applicationId", spark.sparkContext().applicationId());
        sparkInfo.put("master", spark.sparkContext().master());
        sparkInfo.put("defaultParallelism", spark.sparkContext().defaultParallelism());
        sparkInfo.put("version", spark.version());
        result.put("spark", sparkInfo);
        try {
            result.put("flink", flinkGet("overview"));
        } catch (Exception error) {
            result.put("flink", Map.of("status", "UNAVAILABLE",
                    "error", error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage())));
        }
        return result;
    }

    /** Executes bounded, read-only Spark SQL and returns JSON rows for Studio previews. */
    public Map<String, Object> sparkSql(String sql, int requestedLimit) throws Exception {
        if (sql == null || sql.isBlank()) throw new IllegalArgumentException("sql is required");
        SqlNode parsed = SqlParser.create(sql).parseQuery();
        if (parsed.getKind() != SqlKind.SELECT && parsed.getKind() != SqlKind.WITH)
            throw new IllegalArgumentException("Only read-only SELECT queries are supported");
        int limit = Math.max(1, Math.min(requestedLimit, 5000));
        Dataset<Row> result = spark.sql(sql).limit(limit);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String json : result.toJSON().collectAsList()) rows.add(JSON.readValue(json, OBJECT));
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("engine", "spark-sql"); response.put("rows", rows);
        response.put("rowCount", rows.size()); response.put("limit", limit);
        response.put("columns", result.schema().fieldNames());
        return response;
    }

    /** Only named, read-only Flink REST resources are exposed; arbitrary proxy paths are rejected. */
    public Map<String, Object> flinkGet(String resource) throws Exception {
        String path = switch (resource) {
            case "overview" -> "/overview";
            case "jobs" -> "/jobs/overview";
            case "jars" -> "/jars";
            case "taskmanagers" -> "/taskmanagers";
            case "jobmanager-config" -> "/jobmanager/config";
            default -> throw new IllegalArgumentException("Unsupported Flink resource: " + resource);
        };
        return send("GET", path, null);
    }

    public Map<String, Object> flinkJob(String jobId) throws Exception {
        if (jobId == null || !jobId.matches("[A-Fa-f0-9]{32}"))
            throw new IllegalArgumentException("Invalid Flink job id");
        return send("GET", "/jobs/" + jobId, null);
    }

    public Map<String, Object> flinkSubmit(String jarId, Map<String, Object> config) throws Exception {
        if (jarId == null || !jarId.matches("[A-Fa-f0-9]{32}"))
            throw new IllegalArgumentException("Invalid Flink jar id");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("entryClass", config.getOrDefault("entryClass", ""));
        payload.put("parallelism", config.getOrDefault("parallelism", 1));
        payload.put("programArgsList", config.getOrDefault("programArgs", ""));
        return send("POST", "/jars/" + jarId + "/run", JSON.writeValueAsString(payload));
    }

    private Map<String, Object> send(String method, String path, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(flinkUrl + path))
                .timeout(Duration.ofSeconds(20)).header("Accept", "application/json");
        if ("POST".equals(method)) {
            request.header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body == null ? "{}" : body));
        } else request.GET();
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2)
            throw new IllegalStateException("Flink REST returned HTTP " + response.statusCode() + ": "
                    + response.body().substring(0, Math.min(response.body().length(), 1000)));
        return JSON.readValue(response.body(), OBJECT);
    }
}
