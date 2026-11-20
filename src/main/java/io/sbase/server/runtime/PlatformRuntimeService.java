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
                List.of("sql-validation", "spark-sql", "spark-explain", "spark-catalog", "spark-table-cache", "federated-query-planning", "federated-query-execution", "predicate-pushdown")));
        values.add(component("automl", "Scientific AutoML", "Spark ML", "available",
                List.of("classification", "regression", "time-series", "cross-validation", "fairness", "model-cards", "async-jobs", "drift-monitoring")));
        values.add(component("mining", "Scientific Mining", "Apache Mahout", "available",
                List.of("vector-sum", "cosine-similarity", "euclidean-distance", "dot-product", "manhattan-distance", "l2-normalization", "top-k-coordinates")));
        values.add(component("stream-processing", "Stream Processing", "Apache Flink REST adapter", "configured",
                List.of("jar-submit", "jobs-overview", "job-details", "taskmanagers", "jars", "job-cancellation", "savepoint-trigger")));
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

    /** Returns namespaces and the configured object-store/catalog coordinates without exposing credentials. */
    public Map<String, Object> lakehouseStatus() {
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("engine", "apache-iceberg");
        result.put("catalog", "sbase");
        result.put("catalogType", "jdbc-postgresql");
        result.put("warehouse", Optional.ofNullable(System.getenv("SBASE_ICEBERG_WAREHOUSE")).orElse("s3://sbase-warehouse/"));
        result.put("objectStoreEndpoint", Optional.ofNullable(System.getenv("S3_ENDPOINT")).orElse("http://localhost:9000"));
        result.put("status", "UP");
        result.put("namespaces", lakehouseNamespaces());
        return result;
    }

    public List<String> lakehouseNamespaces() {
        return spark.sql("SHOW NAMESPACES IN sbase").collectAsList().stream()
                .map(row -> row.getString(0)).sorted().toList();
    }

    public Map<String,Object> createLakehouseNamespace(String namespace) {
        requireIdentifier(namespace, "namespace");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS sbase." + namespace);
        return Map.of("catalog", "sbase", "namespace", namespace, "status", "AVAILABLE");
    }

    public List<Map<String,Object>> lakehouseTables(String namespace) {
        requireIdentifier(namespace, "namespace");
        List<Map<String,Object>> tables = new ArrayList<>();
        for (Row row : spark.sql("SHOW TABLES IN sbase." + namespace).collectAsList()) {
            Map<String,Object> item = new LinkedHashMap<>();
            item.put("namespace", namespace);
            item.put("name", row.getAs("tableName"));
            item.put("temporary", row.getAs("isTemporary"));
            tables.add(item);
        }
        return tables;
    }

    /** Writes a bounded JSON batch as an Iceberg table; large ingestion belongs in Spark jobs. */
    public Map<String,Object> writeLakehouseRows(String namespace, String table, List<Map<String,Object>> rows) {
        requireIdentifier(namespace, "namespace");
        requireIdentifier(table, "table");
        if (rows == null || rows.isEmpty()) throw new IllegalArgumentException("At least one row is required");
        if (rows.size() > 10000) throw new IllegalArgumentException("API ingestion is limited to 10000 rows; use a Spark ingestion job");
        try {
            List<String> jsonRows = new ArrayList<>();
            for (Map<String,Object> row : rows) jsonRows.add(JSON.writeValueAsString(row));
            var javaRows = new org.apache.spark.api.java.JavaSparkContext(spark.sparkContext())
                    .parallelize(jsonRows, Math.max(1, Math.min(8, jsonRows.size())));
            Dataset<Row> frame = spark.read().json(javaRows);
            String identifier = "sbase." + namespace + "." + table;
            frame.writeTo(identifier).using("iceberg").createOrReplace();
            return Map.of("catalog", "sbase", "namespace", namespace, "table", table,
                    "rowsSubmitted", rows.size(), "columns", Arrays.asList(frame.schema().fieldNames()),
                    "format", "iceberg", "writeMode", "create-or-replace");
        } catch (Exception error) {
            throw new IllegalStateException("Iceberg table write failed: " + error.getMessage(), error);
        }
    }

    public Map<String,Object> lakehouseTablePreview(String namespace, String table, int requestedLimit) throws Exception {
        requireIdentifier(namespace, "namespace");
        requireIdentifier(table, "table");
        int limit = Math.max(1, Math.min(requestedLimit, 1000));
        Dataset<Row> frame = spark.table("sbase." + namespace + "." + table).limit(limit);
        List<Map<String,Object>> rows = new ArrayList<>();
        for (String json : frame.toJSON().collectAsList()) rows.add(JSON.readValue(json, OBJECT));
        return Map.of("catalog", "sbase", "namespace", namespace, "table", table,
                "format", "iceberg", "columns", Arrays.asList(frame.schema().fieldNames()),
                "rows", rows, "rowCount", rows.size(), "limit", limit);
    }

    public List<Map<String,Object>> lakehouseSnapshots(String namespace, String table) throws Exception {
        requireIdentifier(namespace, "namespace");
        requireIdentifier(table, "table");
        List<Map<String,Object>> snapshots = new ArrayList<>();
        Dataset<Row> history = spark.sql("SELECT * FROM sbase." + namespace + "." + table + ".snapshots ORDER BY committed_at DESC LIMIT 100");
        for (String json : history.toJSON().collectAsList()) snapshots.add(JSON.readValue(json, OBJECT));
        return snapshots;
    }

    private static void requireIdentifier(String value, String label) {
        if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_]{0,127}"))
            throw new IllegalArgumentException("Invalid " + label + " identifier");
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

    /** Reports active Spark Structured Streaming queries and their latest progress snapshots. */
    public Map<String, Object> sparkStreams() throws Exception {
        List<Map<String, Object>> streams = new ArrayList<>();
        for (org.apache.spark.sql.streaming.StreamingQuery query : spark.streams().active()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", query.id().toString());
            item.put("runId", query.runId().toString());
            item.put("name", query.name());
            item.put("isActive", query.isActive());
            item.put("status", JSON.valueToTree(query.status()));
            var progress = query.lastProgress();
            item.put("lastProgress", progress == null ? null : JSON.readTree(progress.json()));
            streams.add(item);
        }
        return Map.of("engine", "spark-structured-streaming", "activeQueries", streams,
                "count", streams.size());
    }

    /** Stops only an active Spark stream identified by its stable query UUID. */
    public Map<String, Object> sparkStopStream(String queryId) throws Exception {
        UUID id;
        try { id = UUID.fromString(queryId); }
        catch (Exception invalid) { throw new IllegalArgumentException("Invalid Spark streaming query id"); }
        for (org.apache.spark.sql.streaming.StreamingQuery query : spark.streams().active()) {
            if (query.id().equals(id)) {
                query.stop();
                return Map.of("queryId", queryId, "status", "STOP_REQUESTED");
            }
        }
        throw new NoSuchElementException("Active Spark streaming query not found: " + queryId);
    }

    /** Returns Spark's optimized and physical plans without collecting query results. */
    public Map<String, Object> sparkExplain(String sql) {
        if (sql == null || sql.isBlank()) throw new IllegalArgumentException("sql is required");
        SqlNode parsed;
        try { parsed = SqlParser.create(sql).parseQuery(); }
        catch (Exception error) { throw new IllegalArgumentException("Invalid Spark SQL query", error); }
        if (parsed.getKind() != SqlKind.SELECT && parsed.getKind() != SqlKind.WITH)
            throw new IllegalArgumentException("Only read-only SELECT queries are supported");
        Dataset<Row> frame = spark.sql(sql);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("engine", "spark-sql");
        response.put("schema", frame.schema().json());
        response.put("optimizedPlan", frame.queryExecution().optimizedPlan().toString());
        response.put("physicalPlan", frame.queryExecution().executedPlan().toString());
        return response;
    }

    /** Lists tables visible to the Spark session catalog. */
    public Map<String, Object> sparkTables(String database) throws Exception {
        String selected = (database == null || database.isBlank()) ? "default" : database;
        if (!selected.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid Spark database name");
        List<Map<String, Object>> tables = new ArrayList<>();
        for (org.apache.spark.sql.catalog.Table table : spark.catalog().listTables(selected).collectAsList()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("name", table.name()); item.put("database", table.database());
            item.put("tableType", table.tableType()); item.put("isTemporary", table.isTemporary());
            tables.add(item);
        }
        return Map.of("database", selected, "tables", tables, "count", tables.size());
    }

    /** Caches a validated catalog table for repeated analytical queries. */
    public Map<String, Object> sparkCacheTable(String table) {
        if (table == null || !table.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?"))
            throw new IllegalArgumentException("Invalid Spark table identifier");
        spark.catalog().cacheTable(table);
        return Map.of("table", table, "cached", true);
    }

    /** Cancels a running Flink job through the supported REST endpoint. */
    public Map<String, Object> flinkCancel(String jobId) throws Exception {
        validateFlinkId(jobId, "job");
        return send("PATCH", "/jobs/" + jobId + "?mode=cancel", null);
    }

    /** Requests a Flink savepoint; the target directory must be configured and writable by Flink. */
    public Map<String, Object> flinkSavepoint(String jobId, String targetDirectory) throws Exception {
        validateFlinkId(jobId, "job");
        if (targetDirectory == null || targetDirectory.isBlank())
            throw new IllegalArgumentException("targetDirectory is required");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("target-directory", targetDirectory);
        payload.put("cancel-job", false);
        return send("POST", "/jobs/" + jobId + "/savepoints", JSON.writeValueAsString(payload));
    }

    private static void validateFlinkId(String id, String kind) {
        if (id == null || !id.matches("[A-Fa-f0-9]{32}"))
            throw new IllegalArgumentException("Invalid Flink " + kind + " id");
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
        } else if ("PATCH".equals(method)) {
            request.method("PATCH", HttpRequest.BodyPublishers.noBody());
        } else request.GET();
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2)
            throw new IllegalStateException("Flink REST returned HTTP " + response.statusCode() + ": "
                    + response.body().substring(0, Math.min(response.body().length(), 1000)));
        if (response.body() == null || response.body().isBlank())
            return Map.of("status", "ACCEPTED", "httpStatus", response.statusCode());
        return JSON.readValue(response.body(), OBJECT);
    }
}
