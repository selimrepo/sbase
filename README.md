# SBase — Data Science & Data Mining Hub

Spring Boot + PostgreSQL/JPA metadata + Spark + Mahout + Apache Doris.

## Run
```bash
cp .env.example .env
docker compose up --build -d
```

Open the React workspace at `http://localhost:8080`. For frontend development, run `npm install --prefix frontend` and `npm run dev --prefix frontend`; Vite proxies `/api` and `/actuator` to Spring Boot on port 8080. Configure a JWT bearer token from the workspace profile control when the API is protected by the configured identity provider.

## React data science workspace

The responsive React + TypeScript workspace includes:
- Overview with live metadata inventory, API health, dataset activity and quick actions.
- Dataset catalog with source registration for Doris, PostgreSQL, S3, Parquet and CSV metadata.
- Pipeline studio for versioned Spark workflows, manual execution, live run monitoring and execution history.
- Mining job submission and browser-session run monitoring (the backend exposes job lookup by ID, not a global listing endpoint).
- Experiment registry, model versions, feature-set materialization, executable pipeline schedules and system diagnostics.
- JWT bearer-token configuration, keyboard quick navigation (`Ctrl/Cmd + K`), responsive navigation and dark analytics workspace styling.

The frontend is built by the Node stage in the Dockerfile and copied into Spring Boot's static resources, so the production container serves one application on port 8080. The build workflow compiles the React application before running Gradle tests and packaging.

## Spark pipeline execution

Pipeline definitions are executable DAG-like ordered step lists. The runtime validates one source step, zero or more allow-listed transformations, and exactly one final sink. Each run stores an immutable definition/version snapshot and `QUEUED`, `RUNNING`, `SUCCEEDED` or `FAILED` state in PostgreSQL.

Supported steps:
- Sources: `read-doris` (`table`), `read-parquet` (`uri`) and `read-csv` (`uri`, optional `header`). Doris table identifiers are restricted to SQL identifier syntax. Object storage paths accept `s3a://` and `hdfs://` only.
- Transformations: `filter` (`column`, `operator`, `value`; operators `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `is-null`, `not-null`), `select` (`columns`), `derive` (`operation`: `copy`, `cast-double`, `cast-string`, `log1p` plus `source` and `output`), and `aggregate` (`groupBy`, `metrics`; functions `count`, `sum`, `avg`, `min`, `max`).
- Sinks: `write-doris` (`table`, append mode) and `write-parquet` (`uri`, overwrite mode).

Example pipeline:
```json
{
  "steps": [
    {"type":"read-doris","table":"analytics.events"},
    {"type":"filter","column":"amount","operator":"gte","value":0},
    {"type":"derive","operation":"cast-double","source":"amount","output":"amount_value"},
    {"type":"aggregate","groupBy":["country"],"metrics":[{"function":"sum","column":"amount_value","alias":"revenue"}]},
    {"type":"write-doris","table":"analytics.country_revenue"}
  ]
}
```

Run APIs: `POST /api/pipelines/{id}/runs` (202, returns a run ID), `GET /api/pipelines/runs/{runId}` (poll status/results), and `GET /api/pipelines/{id}/runs` (latest 100 runs). Each run records source/output row counts, output location, step timings and failed-step diagnostics. The React Pipeline Studio uses these endpoints for manual execution and run history.

Schedules require an enabled pipeline. Workers claim due rows with PostgreSQL `FOR UPDATE SKIP LOCKED`, heartbeat their leases while Spark runs, persist the pipeline run ID, compute the next UTC cron occurrence after success, and apply bounded exponential retry on failure. Exhausted schedules are disabled. Migration V5 creates pipeline run history and schedule linkage.

For S3-compatible object stores, configure `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY` and `S3_SECRET_KEY` in the Compose environment. The Docker image distributes the Hadoop S3A and AWS SDK jars to Spark workers together with the MySQL JDBC driver. Spark uses Hadoop S3A support; the `hadoop-aws` version is aligned with Spark's Hadoop 3.3.4 runtime. `hdfs://` paths use the configured Spark/Hadoop environment.

## Core APIs
- Mining jobs: `/api/mining/jobs`
- Dataset catalog: `/api/datasets`
- Pipeline definitions: `/api/pipelines`
- Mahout algorithms: `GET /api/algorithms/mahout`, `POST /api/algorithms/mahout/score` (cosine and Euclidean adapters)
- Platform metadata: `/api/platform/experiments`, `/api/platform/models`, `/api/platform/models/{id}/versions`, `/api/platform/features`, `/api/platform/schedules`, `/api/platform/dashboards?owner=...`
- BI inventory: `GET /api/platform/bi/summary`

## Capability foundations
Migration V3 adds ingestion connector/run metadata, experiment tracking, registered models and immutable numbered model versions, feature-set definitions, persistent schedule definitions, role/permission/subject mapping tables, dashboard and widget metadata. `IngestionConnector` is the extension SPI for Doris/JDBC/object-store/stream connectors. Mahout adapters use Mahout vectors for cosine similarity and expose Euclidean distance alongside it.

## Execution and security services
- Ingestion execution: `POST /api/platform/ingestion/{jdbc|doris|kafka}/runs`; connector types are discovered at `GET /api/platform/ingestion/connectors`. JDBC/Doris use bounded batch inserts. The Kafka connector currently refuses to consume until a durable sink is configured, preventing acknowledged data loss.
- Spark feature materialization: `POST /api/platform/features/materialize` with Parquet input/output URIs and allow-listed operations (`copy`, `cast_double`, `log1p`, `normalize_zscore`, `bucket`).
- S3-compatible artifact storage: `S3ArtifactStore` supports S3/MinIO endpoint overrides, bucket, region and credentials through environment variables.
- Distributed schedule execution: PostgreSQL `FOR UPDATE SKIP LOCKED` claims due pipelines with expiring heartbeating leases, persistent run linkage and bounded exponential retry.
- JWT resource-server validation: configure `JWT_ISSUER_URI`; role and permission claims are mapped to Spring Security authorities. Database role-permission checks are available through `PermissionService`.
- Doris BI datasource: a dedicated JDBC datasource and allow-listed table query service are configured with `DORIS_ALLOWED_TABLES`.

## Production limitations
Pipeline execution supports the documented Spark source, transform and sink steps; it is not an arbitrary code execution engine. Doris sink writes append; Parquet sink writes overwrite the target URI, so choose output paths carefully. S3A/HDFS credentials and network access must be configured for both the Spark driver and workers. CSV/Parquet paths must be reachable from every executor. Feature definitions are not yet served as an online feature store. Model artifact URIs require an external object store and lifecycle policy. Role tables are not yet wired into JWT/OIDC authorization; secure the API behind an authenticated gateway and enforce dataset-level permissions before multi-user production. Dashboard metadata is stored per authenticated JWT subject; arbitrary SQL execution is intentionally not exposed. BI summary currently reports PostgreSQL metadata counts, not Doris query analytics.

Run `gradle clean test bootJar` before deployment. CI must pass before treating this branch as production-ready.

### Automatic data mining

Submit an asynchronous automatic analysis through the existing mining-job API:

```http
POST /api/mining/jobs
Content-Type: application/json

{
  "operation": "auto",
  "sourceTable": "analytics.customer_features",
  "outputTable": "analytics.customer_clusters",
  "k": 3,
  "maxIterations": 30
}
```

The `auto` operation profiles the source using Spark and persists the resulting report in the mining job's `resultSummary`. It detects numeric, categorical and temporal columns; calculates null rates, approximate cardinality, numeric summaries and IQR outlier fences; extracts frequent categorical values; reports strong pairwise numeric correlations; and generates data-quality/modeling recommendations. When there are at least two numeric columns and ten rows, it also fits a deterministic-seed K-means model and appends cluster predictions to the configured Doris output table. Poll `GET /api/mining/jobs/{id}` for QUEUED/RUNNING/SUCCEEDED/FAILED state and the report.

This is unsupervised exploratory mining, not automatic target discovery or a claim of a validated predictive model. Validate generated clusters and recommendations against domain knowledge before operational use.
