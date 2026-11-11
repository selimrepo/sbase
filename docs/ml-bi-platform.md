# ML lifecycle, Feature Store and BI workspaces

This change adds a tenant-scoped ML lifecycle and self-service BI layer to the existing SBase Spring Boot / Spark / PostgreSQL / Doris application. It stays in the existing application and uses PostgreSQL for control-plane metadata, Spark MLlib for training/inference, and Doris for OLAP reads.

## Machine learning

### Experiments and training

- Create tenant-owned experiments with `POST /api/ml/experiments`.
- Submit asynchronous training runs with `POST /api/ml/runs`.
- Supported supervised estimators:
  - Classification: Logistic Regression, Random Forest Classifier.
  - Regression: Linear Regression, Random Forest Regressor.
- Runs use an 80/20 seeded train/validation split and can use Spark MLlib k-fold CrossValidator (1 means no cross-validation; 2–10 are supported).
- Hyperparameter grids support `numTrees`, `maxDepth`, `regParam`, and `maxIter`. Each parameter accepts up to eight candidates. The selected estimator parameter map and fold metrics are stored with the run.
- Training metrics currently include accuracy for classification and RMSE for regression. The run also stores row counts, source table, feature list, label, search space and feature-distribution baseline.
- Training sources are PostgreSQL tables named in `SBASE_ML_ALLOWED_TABLES`. The source must contain `tenant_id`; Spark reads only rows for the authenticated tenant. Feature columns and labels must be numeric for the built-in Spark estimators.

### Registry, lineage and promotion

- Register a successful run as a named model with `POST /api/ml/models`.
- Every registration creates a new immutable model version and retains its run, experiment, source table, feature signature, parameters, evaluation metrics and training baseline.
- `GET /api/ml/models` lists tenant model versions.
- `GET /api/ml/model-versions/{versionId}/lineage` returns the model-to-run-to-experiment provenance.
- `PUT /api/ml/model-versions/{versionId}/stage?stage=STAGING|PRODUCTION|ARCHIVED|NONE` changes lifecycle stage. Promoting a version to PRODUCTION archives the previous production version of that model.

### Serving, batch inference and monitoring

- Create a deployment using `POST /api/ml/deployments`.
- Online prediction: `POST /api/ml/deployments/{deploymentId}/predict` with a `features` object. Inputs are checked against the saved model signature.
- Batch prediction: `POST /api/ml/deployments/{deploymentId}/batch` with 1–10,000 records. The API queues the batch on the bounded mining executor and persists its status and complete JSON result; retrieve it with `GET /api/ml/batches/{batchId}`.
- Monitoring: `GET /api/ml/deployments/{deploymentId}/monitoring` returns 24-hour request/success/failure counts, average and p95 latency, recent sample count and per-feature serving-mean z-scores against the training baseline. A feature is flagged at an absolute z-score of 3 or more. This is a basic numeric mean-shift detector, not a replacement for PSI/KS tests, label-based quality monitoring or alert routing.

### Artifact storage

Spark MLlib artifacts are written below `SBASE_ML_ARTIFACT_ROOT` using tenant/run directories. The path must be persistent and shared by every application node that can serve the model. For multi-node deployments, mount a shared filesystem or provide a common artifact volume. A local ephemeral container filesystem is not a production artifact store.

## Feature Store

- `PUT /api/feature-store/{featureSet}/entities/{entityKey}/versions` atomically appends an immutable offline event-time row and updates the online latest-value record only when the incoming event time is not older.
- `GET /api/feature-store/{featureSet}/entities/{entityKey}/as-of?timestamp=...` reconstructs the latest feature vector whose event time is at or before the requested timestamp.
- `POST /api/feature-store/{featureSet}/entities/{entityKey}/parity?asOf=...` compares the offline point-in-time vector with the online latest-value view, records hashes and differences, and returns the parity result.
- All records are tenant-keyed. Existing online feature values are retained under the legacy tenant during migration; new writes use the authenticated tenant ID.

## Business Intelligence and OLAP

### Visual query design

- `GET /api/bi/catalog/tables` returns configured Doris allow-listed tables and their columns.
- `POST /api/bi/query` accepts a structured definition: table, dimensions, aggregate metrics, filters, sort expressions and result limit.
- Supported aggregates: COUNT, SUM, AVG, MIN, MAX and COUNT_DISTINCT.
- Supported filters: EQ/NE/GT/GTE/LT/LTE, CONTAINS/STARTS_WITH/ENDS_WITH, IN, BETWEEN, IS_NULL and NOT_NULL.
- The server validates table and column identifiers against Doris metadata, escapes values, generates a single-table SELECT, and injects a mandatory `tenant_id` predicate. BI sources without a tenant_id column are rejected.
- Query limits are bounded; estimated complexity is checked before execution. A per-instance semaphore limits concurrent queries. Results are cached in a bounded in-memory TTL cache keyed by tenant and query definition. Every execution/cache hit is audited in PostgreSQL.

### Drill actions

- `POST /api/bi/query/drill-down` adds a selected dimension/value filter and executes a child aggregate query using the requested next dimensions.
- `POST /api/bi/query/drill-through` returns bounded detail rows for the selected table and dimension filters, always retaining the tenant predicate.

### Reports, dashboards and governance

- Save reusable query definitions with `POST /api/bi/reports`; list with `GET /api/bi/reports`; execute with `POST /api/bi/reports/{id}/run`.
- Share a report with `PUT /api/bi/reports/{id}/shares` and a permission of READ, QUERY, EDIT or ADMIN.
- Create dashboards with `POST /api/bi/dashboards`; add report-backed TABLE/BAR/LINE/PIE/KPI/AREA widgets with `POST /api/bi/dashboards/{id}/widgets`.
- Share dashboards with `PUT /api/bi/dashboards/{id}/shares` using READ, EDIT or ADMIN.
- `GET /api/bi/dashboards/{id}/data` resolves the widgets and executes their associated reports under the requesting subject's report grants.

### Scheduled delivery

- `POST /api/bi/schedules` accepts a report ID, Spring cron expression (six fields, including seconds), IANA timezone, CSV/JSON format and optional email recipient.
- A PostgreSQL SKIP LOCKED lease worker exports files below `SBASE_BI_ARTIFACT_ROOT`. Failed schedules retry on the next cron occurrence and are disabled after five consecutive failures.
- Set SMTP_HOST, SMTP_PORT, SMTP_USERNAME and SMTP_PASSWORD in production to enable `mailto:` delivery. File export works without SMTP.
- `GET /api/bi/schedules` lists the current subject's schedules.

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `SBASE_ML_ALLOWED_TABLES` | Comma-separated PostgreSQL training source tables | empty (training disabled until configured) |
| `SBASE_ML_ARTIFACT_ROOT` | Persistent/shared Spark model artifact root | `./data/ml-artifacts` |
| `DORIS_ALLOWED_TABLES` | Comma-separated Doris BI table allowlist | empty (BI catalog empty until configured) |
| `SBASE_BI_MAX_CONCURRENT` | Concurrent BI queries per application instance | 4 |
| `SBASE_BI_CACHE_TTL_MS` | BI in-memory cache TTL | 30000 |
| `SBASE_BI_MAX_ROWS` | Maximum query rows | 5000 |
| `SBASE_BI_MAX_COST` | Preflight query complexity budget | 100000 |
| `SBASE_BI_ARTIFACT_ROOT` | Persistent BI export directory | `./data/bi-reports` |
| `SBASE_BI_SCHEDULER_POLL_MS` | Scheduled export polling interval | 15000 |

The existing ROLE_ADMIN / platform:admin authorities satisfy these endpoints. Other roles must be granted only the required `ml:read`, `ml:write`, `ml:deploy`, `ml:predict`, `bi:read`, `bi:write` and `bi:query` permissions through the existing role-permission management flow.

## Operational notes

- PostgreSQL migration V11 creates the ML registry/run/deployment tables, offline feature history and parity audit, BI report/dashboard/widget/share/schedule tables, query audit and supporting indexes.
- Spark model loading is cached per application process. A shared artifact filesystem is required when deployments can land on different nodes.
- BI cache and semaphore are per process. A multi-node installation therefore has a per-node concurrency budget and per-node cache; use a shared cache/global admission controller if a strict cluster-wide cap is required.
- Scheduled BI export files are written to the configured local/shared artifact root. SMTP delivery sends a notification containing the artifact URI; configure a shared artifact host or storage gateway if recipients must retrieve files remotely.
