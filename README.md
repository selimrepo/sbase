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
- Pipeline studio for versioned JSON workflow definitions.
- Mining job submission and browser-session run monitoring (the backend exposes job lookup by ID, not a global listing endpoint).
- Experiment registry, feature-set inventory, schedule inspection and system diagnostics.
- JWT bearer-token configuration, keyboard quick navigation (`Ctrl/Cmd + K`), responsive navigation and dark analytics workspace styling.

The frontend is built by the Node stage in the Dockerfile and copied into Spring Boot's static resources, so the production container serves one application on port 8080. The build workflow compiles the React application before running Gradle tests and packaging.

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
- Spark offline feature transforms: POST /api/platform/features/materialize with Parquet input/output URIs and allow-listed operations. Pipeline feature_store_write materializes Spark datasets into the online PostgreSQL store.
- S3-compatible artifact storage: `S3ArtifactStore` supports S3/MinIO endpoint overrides, bucket, region and credentials through environment variables.
- Distributed schedule polling: PostgreSQL `FOR UPDATE SKIP LOCKED` claims due jobs with expiring leases, retry counters and exponential backoff.
- JWT resource-server validation: configure `JWT_ISSUER_URI`; role and permission claims are mapped to Spring Security authorities. Database role-permission checks are available through `PermissionService`.
- Doris BI datasource: a dedicated JDBC datasource and allow-listed table query service are configured with `DORIS_ALLOWED_TABLES`.

## Pipeline execution, retries and online features

Pipeline definitions are validated DAGs. Supported steps include doris_read, filter, select, with_column, aggregate, join, doris_write and feature_store_write. Joins accept exactly two dependencies and support inner, left, right and full join types. The engine does not execute arbitrary SQL, Python, shell commands or user-provided code.

Pipeline runs preserve an immutable definition snapshot and a stable run idempotency key. Failed runs enter durable RETRY_WAIT state and are polled from PostgreSQL; attempts are atomically claimed, bounded by sbase.pipeline.retry.max-attempts (default 4), and delayed exponentially (10 seconds up to 15 minutes). Set sbase.pipeline.retry.poll-ms to tune polling. Queued work is redispatched if the process exits before dispatch, and RUNNING work has a six-hour execution lease; expired leases are recovered into the retry queue. A terminal run becomes FAILED after the configured attempt budget is exhausted.

Doris writes are deliberately append-only. A doris_write step must declare idempotencyColumns, and the target must be a Doris UNIQUE KEY table containing those columns. SBase verifies the target key definition and de-duplicates each Spark batch before JDBC append. This makes a retried write safe against duplicate business keys; it is not a substitute for choosing a correct business key or for Doris replication/transaction guarantees. overwrite is rejected to avoid destructive, non-repeatable retries. Doris source and target tables must be listed in DORIS_ALLOWED_TABLES.

Example idempotent write step:

```json
{
  "id": "publish_customers",
  "type": "doris_write",
  "dependsOn": ["customer_rollup"],
  "table": "analytics.customer_rollup",
  "mode": "append",
  "idempotencyColumns": ["customer_id"]
}
```

Online feature serving is backed by PostgreSQL table online_feature_values, keyed by (feature_set, entity_key). Spark pipelines can use feature_store_write with featureSet, entityKeyColumn, optional eventTimeColumn and featureVersion. Materialization stages each batch, then atomically upserts JSONB feature vectors; late-arriving events do not overwrite newer feature timestamps. The REST interface supports:
- PUT /api/online-features/{featureSet}/entities/{entityKey} to upsert a feature vector.
- GET /api/online-features/{featureSet}/{entityKey} to serve a vector.
- GET /api/online-features/{featureSet}/_count for the current entity count.

The Docker image distributes both MySQL and PostgreSQL JDBC drivers to Spark executors. CI includes a live 100,000-row Spark-to-Doris load/read test, duplicate append verification against a Doris UNIQUE KEY table, and a Doris backend stop/recovery check while application liveness is monitored.

## Production hardening boundaries

These features provide durable retry state, idempotent key-based writes and an online feature serving path, but they do not by themselves guarantee exactly-once execution across arbitrary external systems. Select stable business keys, monitor terminal failures, size Spark executors and PostgreSQL pools for workload, and configure Doris replication/compaction for the deployment. Connector SPI still needs concrete source adapters and durable orchestration for every connector type. Model artifact URIs require an external object store and lifecycle policy. Role tables are not yet fully wired to JWT/OIDC authorization; enforce dataset-level permissions before multi-user production. Dashboard metadata is stored per authenticated JWT subject; arbitrary SQL execution is intentionally not exposed. BI summary currently reports PostgreSQL metadata counts, not Doris query analytics.

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
