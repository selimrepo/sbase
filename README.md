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
- Spark feature materialization: `POST /api/platform/features/materialize` with Parquet input/output URIs and allow-listed operations (`copy`, `cast_double`, `log1p`, `normalize_zscore`, `bucket`).
- S3-compatible artifact storage: `S3ArtifactStore` supports S3/MinIO endpoint overrides, bucket, region and credentials through environment variables.
- Distributed schedule polling: PostgreSQL `FOR UPDATE SKIP LOCKED` claims due jobs with expiring leases, retry counters and exponential backoff.
- JWT resource-server validation: configure `JWT_ISSUER_URI`; role and permission claims are mapped to Spring Security authorities. Database role-permission checks are available through `PermissionService`.
- Doris BI datasource: a dedicated JDBC datasource and allow-listed table query service are configured with `DORIS_ALLOWED_TABLES`.

## Production limitations
These capabilities form an executable platform foundation, but deployment still requires environment-specific hardening. The pipeline engine currently supports sequential/single-parent DAG steps: Doris read/write, filter, select, derived columns and grouped aggregation. It intentionally does not execute arbitrary SQL, Python, shell commands, joins or user-provided code. Doris tables must be explicitly listed in DORIS_ALLOWED_TABLES. The default Doris write mode is overwrite; choose append deliberately and account for duplicate delivery on retries. Run records preserve the immutable pipeline definition snapshot and status/result/error timestamps. The scheduler records successful enqueueing, not successful completion of the downstream Spark run. Connector SPI still needs concrete source adapters and durable orchestration for every connector type. Feature definitions are not yet materialized or served online. Model artifact URIs require an external object store and lifecycle policy. Role tables are not yet wired into JWT/OIDC authorization; secure the API behind an authenticated gateway and enforce dataset-level permissions before multi-user production. Dashboard metadata is stored per authenticated JWT subject; arbitrary SQL execution is intentionally not exposed. BI summary currently reports PostgreSQL metadata counts, not Doris query analytics.

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
