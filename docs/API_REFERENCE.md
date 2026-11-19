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
| GET/POST | `/workflows/{workflowId}/deployments` | List or publish version-pinned REST-callable deployments |
| GET | `/deployments?limit=100` | List published workflow services and active-run counts |
| GET | `/deployments/{slug}` | Inspect a published workflow service |
| POST | `/deployments/{slug}/runs` | Invoke a published service; queues the pinned workflow version |
| POST | `/deployments/{slug}/pause` | Stop accepting new invocations without cancelling existing runs |
| POST | `/deployments/{slug}/resume` | Re-enable service invocations |
| GET | `/workflows/{workflowId}/audit?limit=100` | Read workflow publication, invocation and deployment-control audit events |
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

### Published workflow services

A workflow can be published as a stable REST service. Publication pins an immutable workflow version; activating a newer editor version does not silently change the deployed service. Publishing the same slug again for the same workflow updates the pinned version and concurrency policy. A slug cannot be reassigned to a different workflow.

Publish request:

```json
{
  "slug": "monthly-risk-scoring",
  "version": 3,
  "maxConcurrentRuns": 4,
  "maxAttempts": 5
}
```

Omit `version` or set it to `0` to pin the workflow's current active version.

Invoke request:

```json
{
  "idempotencyKey": "risk-batch-2026-11-19",
  "maxAttempts": 5,
  "parameters": {
    "threshold": 0.5,
    "window": 48
  }
}
```

Invocation returns HTTP 202 with a durable `jobId`, the pinned `workflowVersion`, and `QUEUED` status. Use `GET /jobs/{jobId}` and `GET /jobs/{jobId}/events` to monitor execution. The optional `parameters` field must be a JSON object and is persisted with the job. Workflow node configuration can reference typed values with an explicit object such as `{"$parameter":"threshold"}`; missing references fail the run rather than silently becoming null. Reusing an idempotency key with a different parameter object is rejected. Idempotency keys are scoped to the deployment. The configured concurrent-run ceiling is enforced transactionally per deployment; excess invocations receive HTTP 429. Paused services reject new invocations with HTTP 409; already queued or running jobs are not cancelled.

The deployment audit endpoint records publication, invocation, pause and resume events. Current audit actors are recorded as `system` because this API layer does not yet establish authenticated user principals; do not treat the audit actor field as a verified human identity. Protect these management endpoints with an authenticated gateway until first-class SBase identity and workspace RBAC are enabled.


## Scientific AutoML and model governance

| Method | Path | Description |
|---|---|---|
| GET | `/automl/metrics` | Supported task-specific optimization metrics |
| POST | `/automl/data/profile` | Schema, missingness, cardinality, distribution and outlier profile |
| POST | `/automl/science/associations` | Pairwise Pearson/Spearman, Cramer's V and correlation-ratio analysis with Benjamini-Hochberg FDR correction |
| POST | `/automl/science/feature-selection` | Numeric-target Spearman filter ranking with FDR-adjusted q-values |
| POST | `/automl/science/compare-groups` | Welch two-sample t-test, 95% confidence interval and Hedges' g effect size |
| POST | `/automl/science/mann-whitney` | Tie-corrected Mann–Whitney U rank test for two independent groups |
| POST | `/automl/science/kruskal-wallis` | Kruskal–Wallis omnibus rank test for two or more independent groups |
| POST | `/automl/science/anova` | Classical one-way ANOVA with eta-squared and omega-squared |
| POST | `/automl/science/time-series/anomalies` | Rolling median/MAD robust time-series anomaly detection |
| POST | `/automl/science/causal-effect` | Observational propensity-score inverse-probability-weighted ATE estimate |
| POST | `/automl/science/experiment-design` | Two-arm continuous-outcome sample-size and power planning |
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

Scientific mining requests accept an inline `data` array of row objects (1..100000 rows). Association and feature-selection requests accept a `features` array; feature selection additionally requires a numeric `target`. Group comparison requires `groupColumn`, `valueColumn`, `groupA`, and `groupB`. Feature selection supports optional `fdrAlpha` (default 0.05) and `minimumAbsoluteSpearman` (default 0.05). The current feature selector is explicitly a univariate numeric-feature filter; use nested cross-validation for unbiased model selection. Group comparison uses Welch's unequal-variance t-test and reports an effect size; significance is not causality.

Additional scientific research endpoints:

- Mann–Whitney request: `{"data":[{"arm":"control","score":1.2},{"arm":"treatment","score":2.1}],"groupColumn":"arm","valueColumn":"score","groupA":"treatment","groupB":"control"}`. Returns U statistics, tie-corrected asymptotic two-sided p-value and rank-biserial correlation. It does not calculate exact small-sample inference.
- Kruskal–Wallis request: `{"data":[{"arm":"A","score":1},{"arm":"B","score":2}],"groupColumn":"arm","valueColumn":"score"}`. Requires at least two observations per group; returns H, degrees of freedom, chi-square approximation and epsilon-squared. Significant omnibus results need post-hoc tests.
- One-way ANOVA uses the same `data`, `groupColumn`, and `valueColumn` shape. Returns F, degrees of freedom, p-value, eta-squared and omega-squared. This is classical equal-variance ANOVA, not Welch ANOVA.
- Time-series anomaly request: `{"points":[{"timestamp":"2026-01-01T00:00:00Z","value":10.0}],"window":24,"threshold":3.5}`. Points must be in chronological order; window is 5..500. Uses only prior observations for a rolling median/MAD baseline and returns flagged points.
- Causal-effect request: `{"data":[{"treatment":"yes","outcome":12.0,"age":42}],"treatmentColumn":"treatment","treatedValue":"yes","outcomeColumn":"outcome","covariates":["age"]}`. Numeric covariates only; up to 20000 complete rows and 10 covariates. Returns an inverse-probability-weighted ATE estimate, effective sample sizes, propensity clipping count and pre/post covariate standardized mean differences. This is observational analysis, not proof of causality; unmeasured confounding, positivity and propensity-model misspecification remain material risks.
- Experiment-design request: `{"standardizedEffect":0.5,"alpha":0.05,"power":0.8,"allocationRatio":1,"attritionRate":0.1}`. Returns per-arm and total sample-size targets for a two-sided, two-arm continuous-outcome normal approximation. The standardized effect must be justified independently; clustering, repeated measures and multiple testing need separate inflation.

All these endpoints are bounded in input size and return method/assumption notes. They are decision-support calculations; confirm critical analyses with a validated statistical package and a pre-specified analysis plan.

Metric discovery returns classification, regression and time-series metric groups. A metric is not comparable across different task types or validation protocols. PSI/KS are covariate-shift indicators, not proof of model degradation or causality.

## Operational endpoints

- `GET /health`: liveness.
- `GET /ready`: database/control-plane readiness.
- `GET /`: API identity.
