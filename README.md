# SBase

SBase is a KNIME-inspired distributed data science and mining server. The current `develop` branch contains the first runnable workflow execution foundation. It is not yet a fully production-certified enterprise platform.

## Stack

- Scala 2.13 + Gradle
- Apache Pekko Typed, Cluster and Cluster Sharding
- PostgreSQL + Flyway for OLTP, immutable workflow versions, durable jobs, leases and execution events
- Apache Calcite embedded SQL planner/executor for workflow-local relational queries
- Apache Spark SQL for distributed batch operators
- Apache Mahout math for mining operators
- Apache Flink REST for submitting pre-uploaded JARs and inspecting running jobs
- React + React Flow for visual workflow authoring
- PostgreSQL, Cassandra and Solr read-only catalog/query adapters; Cassandra and Solr are optional runtime profiles

Authentication is intentionally out of scope. No login, registration, OAuth/OIDC, MFA or Keycloak components are included. Protect deployments with private networks, firewalls and TLS at the infrastructure edge.

## Run the stack

Requirements: JDK 17+, Gradle 8.14.3+, Docker Engine and Compose v2.

```bash
cp .env.example .env
docker compose up --build
```

- Studio: http://localhost:3000
- API: http://localhost:8080/api/v1
- Health: http://localhost:8080/health
- PostgreSQL: localhost:5432

Run backend tests without Docker except for PostgreSQL Testcontainers tests (Docker required):

```bash
gradle --no-daemon clean test installDist shadowJar

# When running the server directly on the host (instead of Compose):
SBASE_CLUSTER_HOST=127.0.0.1 SBASE_CLUSTER_SEEDS=pekko://sbase@127.0.0.1:25520 gradle run
```

Build the studio:

```bash
cd web
npm install
npm run build
```

## Executable Fat JAR

The Gradle Shadow plugin produces a self-contained executable archive at `build/libs/sbase-server-0.1.0-SNAPSHOT-all.jar`. It includes runtime dependencies and merges `META-INF/services` descriptors for JDBC drivers, logging providers and libraries that use Java service loading. Build it with:

```bash
gradle --no-daemon clean test shadowJar
java -jar build/libs/sbase-server-0.1.0-SNAPSHOT-all.jar
```

The archive uses ZIP64 because Spark and Flink make the dependency bundle large. Optional Geode classes remain compile-only until a runtime Geode connector is implemented.

## Workflow API

Create a workflow with an immutable version 1 definition:

```http
POST /api/v1/workflows
Content-Type: application/json

{
  "name": "Vector mining",
  "definition": {
    "nodes": [
      {
        "id": "sum",
        "operator": "mahout-vector-sum",
        "config": { "values": [1, 2, 3] }
      }
    ],
    "edges": []
  }
}
```

Submit a run (the idempotency key must be unique per logical submission):

```http
POST /api/v1/workflows/{workflowId}/runs
Content-Type: application/json

{ "idempotencyKey": "run-001", "maxAttempts": 3 }
```

Poll the job:

```http
GET /api/v1/jobs/{jobId}
```

Available operators:

- `mahout-vector-sum`: runs a Mahout DenseVector reduction.
- `mahout-cosine-similarity`: calculates cosine similarity between two dense vectors.
- `mahout-euclidean-distance`: calculates Euclidean distance between two dense vectors.
- `calcite-sql`: executes SQL in-process over workflow-local JSON datasets registered as temporary tables. Configure `query` and optional `tables` (an object mapping table names to arrays of row objects). Calcite plans and executes relational projections, filters, joins and aggregations.
- `spark-sql`: executes Spark SQL for distributed batch processing. Set `SPARK_MASTER` to the cluster master URL for distributed execution; defaults to local mode for development.
- `flink-submit`: submits a previously uploaded Flink JAR by `jarId`; optional `entryClass`, `parallelism` and `programArgs` can be supplied.
- `flink-jobs-overview`: reads Flink's jobs overview endpoint.

For Flink, set `FLINK_REST_URL` (Compose profile default is `http://flink-jobmanager:8081`). The Flink JAR upload step is an external artifact publication operation; this adapter invokes the Flink REST run endpoint.

### Calcite SQL operator example

A workflow node can run Calcite SQL over in-memory workflow-local tables:

```json
{
  "id": "query",
  "operator": "calcite-sql",
  "config": {
    "query": "SELECT customer, SUM(amount) AS revenue FROM orders GROUP BY customer ORDER BY revenue DESC",
    "tables": {
      "orders": [
        { "customer": "Ada", "amount": 120.5 },
        { "customer": "Linus", "amount": 90.0 },
        { "customer": "Ada", "amount": 30.0 }
      ]
    }
  }
}
```

Calcite is embedded in the SBase process and requires no separate service. This first adapter materializes node-configured JSON tables. Persistent catalog-backed tables, JDBC pushdown, query resource limits and streaming SQL are follow-up integration work. Use Spark SQL for distributed processing; Calcite's enumerable runtime is an in-process query engine.

## Platform APIs

All platform capabilities are exposed under `/api/v1`:

| Endpoint | Purpose |
|---|---|
| `GET /api/v1/capabilities` | Discover engines and exposed API groups |
| `GET /api/v1/catalogs` | Inspect PostgreSQL tables, Cassandra keyspaces and Solr availability |
| `POST /api/v1/catalogs/postgresql/query` | Bounded read-only SQL with bind parameters |
| `POST /api/v1/catalogs/cassandra/query` | Bounded read-only CQL |
| `POST /api/v1/catalogs/solr/query` | Bounded Solr query by collection |
| `POST /api/v1/query/plan` | Calcite-parser-backed federated cost/physical-plan proposal from supplied table statistics |
| `POST /api/v1/query/execute` | Parallel PostgreSQL/Cassandra/Solr scans and Spark distributed SQL joins |
| `POST /api/v1/automl/train` | Cross-validated Spark ML search, durable experiment and model registration |
| `GET /api/v1/automl/experiments` and `GET /api/v1/automl/experiments/{id}` | Experiment history and trial metrics |
| `GET /api/v1/models` and `GET /api/v1/models/{id}` | Model registry |
| `POST /api/v1/models/{id}` | Batch prediction using a registered model |
| `POST /api/v1/models/{id}/archive` | Archive a model |
| `PUT /api/v1/models/aliases/{alias}` | Promote/rollback a deployment alias to a ready model |
| `POST /api/v1/workflows` | Validate and persist an immutable workflow version |
| `POST /api/v1/workflows/{workflowId}/runs` | Submit an idempotent distributed workflow run |
| `GET /api/v1/jobs/{jobId}` | Read job status and result |

PostgreSQL query request:

```json
{
  "sql": "SELECT customer_id, SUM(amount) AS revenue FROM sales WHERE amount > ? GROUP BY customer_id",
  "parameters": [100],
  "limit": 1000
}
```

Cassandra and Solr use `{"cql":"SELECT ...","limit":500}` and
`{"collection":"products","q":"category:books","limit":100}` respectively.
The adapters enforce read-only statements and result limits. Cassandra and Solr are
optional Compose profiles; enable them with `docker compose --profile nosql --profile search up --build`.
Set `SBASE_CASSANDRA_CONTACT_POINT`, `SBASE_CASSANDRA_PORT`,
`SBASE_CASSANDRA_DATACENTER` and `SBASE_SOLR_URL` for external deployments.

### Federated query planning

The planner parses SQL with Calcite, matches referenced tables against caller-supplied
catalog statistics, orders scans by estimated cardinality, selects a source-local or
cross-source exchange strategy, and estimates scan/exchange cost and parallelism.
Example:

```json
{
  "sql": "SELECT c.id, o.total FROM customers c JOIN orders o ON c.id = o.customer_id",
  "catalog": {
    "customers": {"source":"postgresql","rows":10000,"bytes":1200000,"partitions":4},
    "orders": {"source":"cassandra","rows":8000000,"bytes":900000000,"partitions":64}
  }
}
```

This API returns an optimization proposal, not an executed federated SQL result. The
current version does not yet perform adaptive runtime re-optimization, cross-source
join execution, or Calcite JDBC/Cassandra/Solr adapter pushdown. Those require a
runtime exchange layer and persistent statistics collection; the planner does not
pretend that a generated plan is already executable.

### AutoML

`POST /api/v1/automl/train` accepts `task` (`classification` or `regression`),
`label`, numeric `features`, inline `data` rows and optional `maxCandidates`
(up to 3). It evaluates Spark ML Logistic Regression, Random Forest and Decision Tree
candidates for classification, or Linear Regression, Random Forest and Decision Tree
for regression, using a deterministic 80/20 holdout split. The response contains
per-candidate metrics and the best candidate.

Training now runs a bounded cross-validation and hyperparameter grid for each selected
Spark ML algorithm, scores candidates on a held-out validation split, persists the winning
Spark PipelineModel under `SBASE_MODEL_REGISTRY_PATH`, and writes experiment/trial/model
metadata to PostgreSQL. The API exposes experiment history/details, model metadata,
prediction, archival and transactional deployment aliases. Docker Compose mounts a
dedicated `models_data` volume. Models are versioned per experiment; aliases can be
repointed for rollback. The current feature pipeline expects numeric input columns;
categorical encoding, time-series tasks, distributed feature stores, drift monitoring
and asynchronous cancellation are follow-up capabilities.

### Federated execution

`POST /api/v1/query/execute` accepts a SQL query plus named source scans. Source reads
run concurrently with per-source and overall row caps; Calcite parses and validates
the read-only query; the resulting tables are registered in an isolated Spark SQL
session. Spark Catalyst performs join planning and Adaptive Query Execution can choose
broadcast/shuffle strategies and distribute joins to the configured Spark cluster.
This is an executable cross-source join path, not only a plan proposal. Connector
pushdown is controlled by each source scan query supplied in the request; automatic
SQL-to-CQL/Solr predicate decomposition and streaming spill-to-disk are not yet included.

## Durable execution design

1. Workflow definitions are JSONB snapshots with SHA-256 checksums and immutable version references.
2. Job submission is idempotent and stored transactionally in PostgreSQL.
3. Workers claim jobs using `FOR UPDATE SKIP LOCKED`.
4. Each claim receives a fresh database-generated fencing token and lease expiry.
5. Sharded Pekko worker actors execute the workflow and periodically heartbeat the lease.
6. A worker that loses its lease cannot commit a stale result.
7. Expired leases are recovered; exponential retry backoff is applied to failed attempts.
8. Jobs that exhaust their retry budget are terminally failed instead of being requeued forever.
9. Execution events are recorded for queue, lease, success and failure transitions.

Pekko seed nodes and advertised/bind addresses can be configured with `SBASE_CLUSTER_SEEDS`, `SBASE_CLUSTER_HOST`, `SBASE_CLUSTER_PORT`, `SBASE_CLUSTER_BIND_HOST` and `SBASE_CLUSTER_BIND_PORT`. Use stable, directly reachable node addresses for multi-node deployments.

## CI

GitHub Actions runs:

- Gradle Scala compilation, JUnit unit tests, PostgreSQL Testcontainers integration tests and application distribution packaging.
- React production build.
- Backend Docker image build.

The Compose file includes optional development profiles for Cassandra, Solr, Spark and Flink. They are not an HA/DR production topology.

## Roadmap

- [x] Gradle Scala + Pekko service foundation
- [x] Pekko Cluster Sharding worker region
- [x] PostgreSQL durable queue, lease fencing, heartbeats, retries and expired-lease recovery
- [x] Mahout vector mining, embedded Apache Calcite SQL and Spark SQL operators
- [x] Flink REST job submission and job overview
- [x] React Flow workflow graph editor and job polling
- [ ] Node input/output contracts and persisted per-node execution previews
- [ ] Spark structured streaming and Flink checkpoint/savepoint lifecycle management
- [x] Read-only PostgreSQL, Cassandra and Solr catalog/query APIs
- [x] Calcite-backed federated cost/plan proposal API
- [x] Spark ML baseline AutoML classification/regression candidate search API
- [ ] Executable cross-source joins, adaptive distributed query optimization and JDBC/source pushdown
- [ ] Persistent AutoML experiments, model registry, cross-validation and automated feature engineering
- [ ] Geode production connector implementation
- [ ] Outbox dispatcher, scheduler, resource quotas and cancellation
- [ ] OpenTelemetry metrics/traces, alerting, HA/DR and restore drills
- [ ] Multi-node chaos, load and week-long soak verification

## License

Apache License 2.0. See `LICENSE`.
