# SBase API reference

All routes below are rooted at `/api/v1`. The API is intentionally capability-driven: discover components and operator schemas before building workflow editors.

## Platform discovery

| Method | Path | Description |
|---|---|---|
| GET | `/components` | Component inventory, technologies, availability and exposed features |
| GET | `/capabilities` | Machine-readable platform capability groups |
| GET | `/operators` | Stable list of workflow operator IDs |
| GET | `/operators/catalog` | Operator descriptions, categories, output names and required/optional config keys |
| GET | `/operators/{name}` | Descriptor for one operator; 404 for unknown IDs |
| GET | `/engines` | Spark runtime information and Flink REST availability |
| GET | `/engines/flink/{resource}` | Read-only Flink resources: `overview`, `jobs`, `jars`, `taskmanagers`, `jobmanager-config` |

## Query and compute

### Spark SQL

`POST /engines/spark/sql`

```json
{
  "sql": "SELECT region, AVG(revenue) AS average_revenue FROM sales GROUP BY region",
  "limit": 500
}
```

Only read-only `SELECT` and `WITH` queries are accepted. Results are capped at 5,000 rows.

### Flink

- `GET /engines/flink/jobs/{jobId}` inspects one Flink job.
- `POST /engines/flink/jars/{jarId}/run` submits an uploaded JAR.

```json
{
  "entryClass": "com.example.StreamingJob",
  "parallelism": 2,
  "programArgs": "--input events"
}
```

Flink job and JAR IDs are validated before the REST request. Flink must be reachable at `FLINK_REST_URL`.

### Catalog schema inspection

`GET /catalogs/postgresql/columns?schema=public&table=orders`

Returns PostgreSQL column name, SQL type, JDBC type, size, nullability, ordinal and default metadata. Existing catalog discovery and bounded read-only query endpoints remain available for PostgreSQL, Cassandra and Solr.

### Federated query

- `POST /query/plan` proposes a cost-aware plan from supplied catalog statistics.
- `POST /query/execute` executes bounded source scans and a Spark SQL join.
- `POST /catalogs/postgresql/query`, `/catalogs/cassandra/query`, and `/catalogs/solr/query` execute bounded read-only source queries.

## Workflow Studio and execution

| Method | Path | Description |
|---|---|---|
| POST | `/workflows/validate` | Validate a graph without persisting it; returns topological execution order |
| GET | `/workflows?limit=100` | List workflow summaries |
| POST | `/workflows` | Create a workflow and immutable version 1 |
| GET | `/workflows/{workflowId}` | Read workflow metadata and active definition |
| GET | `/workflows/{workflowId}/versions` | List immutable versions |
| GET | `/workflows/{workflowId}/versions/{version}` | Read a specific immutable version |
| POST | `/workflows/{workflowId}/versions` | Create a new immutable version |
| PUT | `/workflows/{workflowId}/versions/{version}/activate` | Activate a version |
| GET | `/workflows/{workflowId}/runs?limit=100` | List workflow runs |
| POST | `/workflows/{workflowId}/runs` | Submit an idempotent run |
| GET | `/jobs?limit=100` | List recent jobs across workflows |
| GET | `/jobs/{jobId}` | Read job state and result |
| GET | `/jobs/{jobId}/events` | Read execution and lease events |
| GET/POST | `/workflows/{workflowId}/schedules` | List or create interval schedules |
| GET | `/schedules` | List schedules |
| POST | `/schedules/{scheduleId}/pause` | Pause a schedule |
| POST | `/schedules/{scheduleId}/resume` | Resume a schedule |

Validation request:

```json
{
  "definition": {
    "nodes": [
      {"id": "source", "operator": "dataset-values", "config": {"values": [1, 2, 3]}},
      {"id": "sum", "operator": "mahout-vector-sum", "config": {}}
    ],
    "edges": [{"source": "source", "target": "sum"}]
  }
}
```

The validator checks node IDs, supported operators, edge references and cycles. It does not execute nodes or persist the graph.

## Scientific AutoML and model governance

| Method | Path | Description |
|---|---|---|
| GET | `/automl/metrics` | Supported task-specific optimization metrics |
| POST | `/automl/data/profile` | Schema, missingness, cardinality, distribution and outlier profile |
| POST | `/automl/monitoring/statistical-drift` | PSI, KS and missing-rate drift comparison |
| GET | `/automl/monitoring/statistical-runs` | Recent statistical drift runs |
| POST | `/automl/train` | Synchronous scientific AutoML |
| POST | `/automl/jobs` | Asynchronous AutoML submission |
| GET/POST | `/automl/jobs/{jobId}` | Inspect or cancel an AutoML job |
| GET | `/automl/experiments` | List experiments |
| GET | `/automl/experiments/{experimentId}` | Experiment and trial details |
| GET | `/models` | Model registry |
| GET | `/models/{modelId}` | Model metadata |
| POST | `/models/{modelId}` | Batch prediction |
| GET | `/models/{modelId}/card` | Scientific model card |
| GET/POST | `/models/{modelId}/monitoring` | Read or create operational drift runs |
| GET | `/models/{modelId}/statistical-drift` | Statistical drift history |
| GET | `/models/aliases` | List deployment aliases |
| GET | `/models/aliases/{alias}` | Resolve one alias |
| PUT | `/models/aliases/{alias}` | Promote a READY model |
| POST | `/models/aliases/{alias}/rollback` | Guarded rollback |
| POST | `/models/{modelId}/archive` | Archive a READY model |

Metric discovery returns classification, regression and time-series metric groups. A metric is not comparable across different task types or validation protocols. PSI/KS are covariate-shift indicators, not proof of model degradation or causality.

## Operational endpoints

- `GET /health`: liveness.
- `GET /ready`: database/control-plane readiness.
- `GET /`: API identity.
