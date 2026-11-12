# SBase

**Self-hosted Enterprise Data Platform for Data Integration, Workflow Orchestration, Data Mining and BI.**

SBase brings data ingestion, pipeline execution, exploratory data mining, feature materialization and operational analytics together in one deployable platform. It is designed for teams that need to connect business data, automate repeatable processing and make analytical results accessible without splitting the workflow across unrelated tools.

SBase is currently delivered as a **self-hosted platform**. Deploy it in your own environment and keep infrastructure and data under your control. A hosted multi-tenant SaaS offering is not currently provided.

[Repository](https://github.com/selimrepo/sbase) · [Issues](https://github.com/selimrepo/sbase/issues) · [Pull requests](https://github.com/selimrepo/sbase/pulls)

---

## Why SBase?

Organizations often operate ERP, CRM, operational databases, files and analytical stores independently. SBase provides a common workspace and execution layer for connecting those sources and managing data workflows.

- **Connect data:** register datasets and work with JDBC, Doris, PostgreSQL, S3-compatible storage, Parquet and CSV metadata.
- **Build pipelines:** define validated, versioned workflow DAGs and execute supported transformations.
- **Run data mining:** submit asynchronous exploratory analysis and inspect job results.
- **Serve features:** materialize feature vectors into a PostgreSQL-backed online feature store.
- **Understand analytics:** inspect Doris physical-table statistics and query activity alongside platform metadata.
- **Operate centrally:** monitor jobs, schedules, datasets, experiments, models and platform health from a React workspace.

## Platform capabilities

### Data integration and catalog

The dataset catalog records data assets and their source metadata. Ingestion endpoints expose JDBC, Doris, Kafka, Debezium CDC, REST and file connector types. Kafka and Debezium events are persisted to a PostgreSQL JSONB landing sink before Kafka offsets are committed. REST JSON and CSV/JSONL/NDJSON files use the same durable landing and idempotency infrastructure. Connector executions have bounded retries and a persistent dead-letter table.

The `IngestionConnector` interface is an extension point for additional connector implementations. Connector registration metadata and ingestion run metadata are persisted in PostgreSQL.

### Workflow and pipeline execution

Pipeline definitions are versioned JSON DAGs. The current execution engine supports:

| Step | Purpose |
|---|---|
| `doris_read` | Read an allow-listed Doris table |
| `filter` | Filter rows using supported expressions |
| `select` | Project selected columns |
| `with_column` | Derive a column using supported operations |
| `aggregate` | Group and aggregate data |
| `join` | Join two upstream datasets |
| `union` | Combine two upstream datasets |
| `doris_write` | Append results to a Doris UNIQUE KEY table |
| `feature_store_write` | Materialize features into the online feature store |

Joins accept exactly two dependencies and support inner, left, right and full join types. Union accepts two dependencies and supports schema-aware union-all or union-distinct; chain additional inputs through successive merge steps.

The engine validates pipeline structure and supported operations. It does **not** execute arbitrary SQL, Python, shell commands or user-supplied code.

#### Durable execution and retries

Pipeline runs retain an immutable definition snapshot and a stable run idempotency key. Transient database, network and timeout failures can be retried; invalid definitions, schema errors and authentication failures are terminal.

- Retryable runs enter a durable PostgreSQL `RETRY_WAIT` state.
- Attempts are claimed atomically and bounded by `sbase.pipeline.retry.max-attempts` (default: 4).
- Retry delays use exponential backoff, from 10 seconds up to 15 minutes.
- Polling is configurable through `sbase.pipeline.retry.poll-ms`.
- Queued work can be redispatched after process restart.
- Running work uses a six-hour execution lease; expired leases are recovered into the retry queue.

These mechanisms improve recoverability but do not promise exactly-once execution across arbitrary external systems.

#### Safe Doris writes

Doris writes are append-only. A `doris_write` step must declare `idempotencyColumns`, and the destination must be a Doris UNIQUE KEY table containing those columns. SBase verifies the target key definition and de-duplicates each Spark batch before JDBC append. Destructive `overwrite` writes are rejected.

Choose stable business keys carefully. Key-based de-duplication does not replace Doris replication, transaction or storage guarantees. Source and target tables must be included in `DORIS_ALLOWED_TABLES`.

Example:

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

### Data mining and machine learning foundations

Asynchronous mining jobs are submitted through the mining API and can be monitored by job ID. The automatic exploratory operation profiles a source table using Spark and stores its report in the job's `resultSummary`.

The current automatic profile can include:

- Numeric, categorical and temporal column detection.
- Null rates and approximate cardinality.
- Numeric descriptive summaries and IQR-based outlier fences.
- Frequent categorical values.
- Strong pairwise numeric correlations.
- Data-quality and modeling recommendations.
- Deterministic-seed K-means clustering when the source has at least two numeric columns and ten rows.

When clustering is applicable, predictions are appended to the configured Doris output table. This is unsupervised exploratory analysis, not automatic target discovery or a validated predictive model. Review clusters and recommendations with domain knowledge before operational use.

Mahout adapters expose cosine similarity and Euclidean distance operations.

### Feature engineering and online feature serving

Spark offline feature transformations accept Parquet input/output URIs and an allow-listed set of operations. Pipeline `feature_store_write` materializes selected features into the online PostgreSQL store.

Online feature values are stored in `online_feature_values`, keyed by `(feature_set, entity_key)`. Materialization stages each batch and atomically upserts JSONB feature vectors. Older event timestamps do not overwrite newer feature values.

Available REST operations:

| Method | Endpoint | Purpose |
|---|---|---|
| PUT | `/api/online-features/{featureSet}/entities/{entityKey}` | Upsert a feature vector |
| GET | `/api/online-features/{featureSet}/{entityKey}` | Retrieve a feature vector |
| GET | `/api/online-features/{featureSet}/_count` | Count entities in a feature set |

### BI and analytical observability

SBase separates PostgreSQL control-plane metadata from Doris analytical-store statistics.

The BI summary endpoint, `GET /api/platform/bi/summary`, reports:

- PostgreSQL metadata inventory.
- Doris physical table count.
- Estimated row counts and data/index storage.
- 24-hour query count and average query time, when Doris query audit logging is available.
- Scanned bytes and rows, when audit metrics are available.
- Per-table statistics for the React overview and system diagnostics.

Doris table statistics can remain available when query audit logging is disabled or inaccessible. In that case, query-audit metrics are reported as unavailable rather than being presented as zero-valued analytics. Arbitrary SQL execution is not exposed by the BI summary.

### Workspace and operations

The React + TypeScript workspace provides:

- Overview with platform metadata, Doris analytics, API health and quick actions.
- Dataset catalog and source metadata registration.
- Pipeline studio for JSON workflow definitions.
- Mining job submission and browser-session run monitoring.
- Experiment registry and model inventory with numbered model versions.
- Feature-set inventory and schedule inspection.
- BI overview and system diagnostics.
- JWT bearer-token configuration and responsive navigation.
- Keyboard quick navigation with `Ctrl/Cmd + K`.

The workspace monitors mining jobs by ID; the backend does not currently provide a global mining-job listing endpoint.

---

## Architecture at a glance

| Layer | Technology / responsibility |
|---|---|
| Application API | Java, Spring Boot |
| Persistence and control plane | PostgreSQL, Spring Data JPA |
| Analytical storage | Apache Doris |
| Distributed processing | Apache Spark |
| Similarity algorithms | Apache Mahout |
| Web workspace | React, TypeScript |
| Artifact storage | S3-compatible object storage, including MinIO |
| Deployment | Docker and Docker Compose |

PostgreSQL stores platform metadata, workflow state, schedules and online feature values. Doris is used as the analytical data store. Spark provides distributed offline processing and supported pipeline transformations. The React workspace is served by the Spring Boot application in the production container.

## Quick start

### Requirements

- Docker Engine
- Docker Compose plugin
- Sufficient memory and disk for the application and its configured data services

### Start the platform

```bash
git clone https://github.com/selimrepo/sbase.git
cd sbase
cp .env.example .env
docker compose up --build -d
```

Open the workspace:

- Application: [http://localhost:8080](http://localhost:8080)
- Actuator health: [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health)

The exact services and exposed ports depend on the checked-in Compose configuration and environment settings. Review `.env.example` before starting a non-development deployment.

### Stop the platform

```bash
docker compose down
```

To remove persisted Compose volumes as well, use `docker compose down -v`. This deletes the data stored in those volumes; do not run it if you need to preserve local databases or artifacts.

## Production deployment

The default Compose stack is intended for local evaluation and CI. A hardened single-host deployment overlay, PostgreSQL backup/restore scripts, Prometheus metrics configuration, and explicit production limitations are documented in [docs/production-readiness.md](docs/production-readiness.md).

The production overlay binds the application to loopback by default, removes published database/analytics ports, requires explicit credentials and JWT issuer configuration, provisions a dedicated Doris application account, and protects Prometheus scraping with a monitoring JWT. It is not a substitute for a multi-node Doris/Kafka topology, tenant isolation, off-host backups, or a tested disaster-recovery plan.

## Frontend development

The production Dockerfile builds the React application with Node and copies the generated assets into Spring Boot's static resources. The production deployment therefore serves the workspace and API from one application endpoint on port 8080.

For local frontend development:

```bash
npm install --prefix frontend
npm run dev --prefix frontend
```

The Vite development server proxies `/api` and `/actuator` requests to Spring Boot on port 8080. Start the backend separately when using the frontend development server.

## API overview

| Area | Endpoint | Description |
|---|---|---|
| Mining | `/api/mining/jobs` | Submit asynchronous mining jobs |
| Mining | `/api/mining/jobs/{id}` | Inspect a mining job and its result |
| Datasets | `/api/datasets` | Dataset catalog |
| Pipelines | `/api/pipelines` | Pipeline definitions |
| Ingestion | `/api/platform/ingestion/connectors` | Discover connector types |
| Ingestion | `/api/platform/ingestion/{jdbc\|doris\|kafka}/runs` | Submit supported ingestion runs |
| Mahout | `GET /api/algorithms/mahout` | List exposed Mahout adapters |
| Mahout | `POST /api/algorithms/mahout/score` | Run cosine / Euclidean scoring |
| Experiments | `/api/platform/experiments` | Experiment metadata |
| Models | `/api/platform/models` | Registered models |
| Model versions | `/api/platform/models/{id}/versions` | Numbered model versions |
| Features | `/api/platform/features` | Feature-set definitions |
| Schedules | `/api/platform/schedules` | Persistent schedule definitions |
| Dashboards | `/api/platform/dashboards?owner=...` | Dashboard metadata for an owner |
| BI | `GET /api/platform/bi/summary` | PostgreSQL metadata and Doris analytics summary |
| Online features | `/api/online-features/{featureSet}/...` | Online feature read/write/count |

For request and response schemas, inspect the application's API documentation or the controller definitions in the source tree.

## Configuration

Configuration is supplied through Spring application properties and environment variables. The following variables are used by the documented integrations; consult `.env.example` and `application.yml` for the current defaults and complete list.

| Variable / property | Purpose |
|---|---|
| `JWT_ISSUER_URI` | JWT issuer used by the resource server |
| `DORIS_ALLOWED_TABLES` | Allow-list of Doris source and destination tables |
| `DORIS_DATABASE` | Doris database used by the BI analytics summary (default: `analytics`) |
| `sbase.pipeline.retry.max-attempts` | Maximum pipeline retry attempts (default: 4) |
| `sbase.pipeline.retry.poll-ms` | Pipeline retry polling interval |
| S3/MinIO endpoint, bucket, region and credentials | S3-compatible artifact storage configuration |

Do not commit production credentials, signing keys or access tokens. Use deployment-specific secret management and restrict network access to database and object-store services.

## Security and production considerations

- Configure JWT issuer validation through `JWT_ISSUER_URI` when the API is protected by an identity provider.
- JWT role and permission claims are mapped to Spring Security authorities. A database-backed `PermissionService` is available, but the presence of role/permission metadata tables should not be interpreted as complete dataset-level authorization.
- Dashboard metadata is scoped to the authenticated JWT subject.
- Doris access is restricted through the configured table allow-list.
- Pipeline definitions are validated; arbitrary SQL, shell commands and user-provided program execution are not supported.
- Keep PostgreSQL, Doris, object storage and management ports private to trusted networks.
- Configure backups, retention, TLS, secrets, resource limits and monitoring for each deployment.
- Size Spark executors, PostgreSQL connection pools and Doris resources for the expected workload.
- Configure Doris replication, compaction and storage policies according to the deployment's availability requirements.

### Current boundaries

SBase includes durable retry state, idempotency-key-based Doris append behavior and online feature serving. These do not guarantee exactly-once processing across arbitrary external systems.

The connector SPI is an extension point; it does not mean every possible source connector has a complete production-ready adapter and durable execution path. Kafka and Debezium consumers persist each batch and checkpoint transactionally before committing source offsets. This is at-least-once delivery with idempotent landing, not a distributed exactly-once guarantee. Model artifact URIs require an external object store and lifecycle policy. Dataset-level authorization and tenant isolation must be completed and verified before exposing a shared multi-user deployment to untrusted customers.

SBase is currently a self-hosted platform, not a hosted multi-tenant SaaS service. SaaS billing, tenant provisioning, hosted operations and service-level commitments are not represented as available product capabilities.

## Automatic mining example

Submit an asynchronous exploratory analysis:

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

Poll the returned job identifier:

```http
GET /api/mining/jobs/{id}
```

The job progresses through states such as `QUEUED`, `RUNNING`, `SUCCEEDED` and `FAILED`. On success, inspect `resultSummary` for the generated profile and recommendations. K-means output is produced only when the input meets the minimum numeric-column and row-count requirements described above.

## CI and testing

The repository uses GitHub Actions to build the React workspace, run Gradle tests and exercise the Docker Compose integration environment. Integration coverage includes Spark-to-Doris data movement, duplicate-key behavior for Doris UNIQUE KEY writes and Doris backend recovery scenarios.

Run the project tests locally with the Gradle wrapper:

```bash
./gradlew test
```

Run the frontend build with:

```bash
npm ci --prefix frontend
npm run build --prefix frontend
```

The integration workflow may require Docker resources and can take substantially longer than unit tests.

## Deployment and commercial model

SBase is suitable for evaluation and deployment inside an organization's own infrastructure. Teams can begin with a self-managed installation and add professional services around environment setup, connector configuration, pipeline implementation, BI reporting, performance tuning and ongoing support.

The project does not currently claim to provide a hosted SaaS subscription. A future hosted offering would require additional work around tenant isolation, tenant-aware authorization, provisioning, metering, billing, operational monitoring, backups, upgrades and service-level objectives.

## Contributing

Issues and pull requests are welcome.

1. Open an issue describing the problem or proposed capability.
2. Keep changes focused and include tests for behavior changes.
3. Run the relevant Gradle and frontend checks.
4. Describe configuration changes, migration implications and operational impact in the pull request.

Please avoid including credentials, customer data or production connection details in issues and test fixtures.

## License

No license file is currently identified in this repository. Until a license is added, do not assume that the source is available for unrestricted reuse, redistribution or commercial modification. Repository visibility alone does not grant an open-source license.
