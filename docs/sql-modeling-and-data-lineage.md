# SQL Modeling, Data Lineage and Governance

## Status and scope

This document is the implementation contract for SBase's next data-platform capability set. It is an architecture and acceptance specification, not a claim that the runtime features are already implemented.

Keep the existing Spring Boot modular monolith, PostgreSQL control plane, Doris analytical engine, Spark pipeline runtime and React workspace. Reuse tenant context, resource ACL, pipeline run/lease/retry, BI report/dashboard and Flyway conventions. Do not introduce microservices for these capabilities.

## Product outcome

A user must be able to trace a dashboard metric back to its report, model, SQL expression, pipeline node, source column and originating source system. The graph must also answer impact questions before a schema or model change is deployed.

## 1. SQL workspace

### Editor

- Syntax highlighting, formatting, autocomplete and schema/table/column discovery.
- CTEs, nested queries, window functions, joins, aggregates and named parameters.
- Typed parameters with required/default/secret metadata. Bind values using JDBC prepared statements; never interpolate values into SQL.
- Explain plans, query history, saved queries, ownership, sharing, tags and execution metrics.
- Server-side result pagination, row/byte caps and CSV/JSON export.
- Explicit dialect selection, initially PostgreSQL and Doris.

### Parser-based validation and execution safety

Use a real SQL parser and AST inspection, never regular expressions. The validator must:

1. Parse exactly one statement and reject trailing statements.
2. Allow read-only SELECT and WITH...SELECT in the interactive editor.
3. Reject DDL, DML, transaction control, session mutation, COPY, CALL, DO, external file access and unsafe functions unless a separately permissioned administrative capability allows them.
4. Resolve every relation and column against tenant-visible catalog metadata and engine allow-lists.
5. Reject cross-tenant or unregistered objects before submission.
6. Require typed bind parameters for values. Identifiers must be resolved from catalog metadata, never accepted as bindable strings.
7. Enforce per-user/tenant concurrency, statement timeout, memory/scan budgets where supported, result row/byte caps and cancellation.
8. Use read-only credentials and transactions for preview and query execution.
9. Audit actor, tenant, SQL hash, normalized SQL, redacted parameters, referenced objects, plan metadata, duration, rows, bytes, status and failure category.
10. Apply identical policy to API execution, saved/scheduled queries and pipeline SQL nodes.

EXPLAIN ANALYZE is query execution, not harmless validation; require explicit permission and normal resource limits.

## 2. SQL pipeline node

Add a typed sql node to the existing pipeline DAG. Example:

~~~json
{
  "id": "enrich_orders",
  "type": "sql",
  "engine": "DORIS",
  "dependsOn": ["orders_source", "customers_source"],
  "query": "WITH recent AS (SELECT * FROM {{input.orders}} WHERE order_date >= :since) SELECT r.order_id, c.segment FROM recent r JOIN {{input.customers}} c ON c.customer_id = r.customer_id",
  "parameters": [
    { "name": "since", "type": "TIMESTAMP", "value": "2026-01-01T00:00:00Z" }
  ],
  "output": { "mode": "TEMP_VIEW", "name": "enriched_orders" },
  "timeoutSeconds": 300
}
~~~

Contract:

- Inputs are explicit DAG dependencies. The controlled input relation syntax is resolved by the compiler, not arbitrary textual substitution.
- Validate SQL, parameters, relation references and output contract before acquiring execution resources.
- Integrate with run leases, heartbeat, retry classification, step logs, cancellation and idempotency.
- Persist normalized SQL, dialect, input/output schema fingerprints and compiler version in immutable pipeline snapshots.
- Write to run-scoped staging and publish atomically where supported. Do not blindly retry non-idempotent side effects.
- Reject unknown node types until the runtime executor exists; never accept a node that cannot execute.

## 3. SQL models and dbt interoperability

### Model registry

Versioned models require stable project/name, SQL source, dialect, description, tags, owner/team/domain, declared dependencies, external sources, materialization config, unique keys, incremental strategy, partition/order keys, freshness, timeout, column descriptions/types/tests/classifications, compiled SQL, source and manifest checksums, dependency snapshot and run history.

Lifecycle: draft, validated, ready, running, successful, failed, deprecated.

### dbt compatibility

Do not advertise full dbt compatibility until dbt project, manifest and adapter contracts pass compatibility tests. Deliver in increments:

1. Import/export dbt project layout, model SQL, schema YAML, sources, tags and documentation blocks.
2. Parse manifest artifacts and preserve model/source/column metadata and dependency edges.
3. Support a documented subset: ref(), source(), var(), model config, CTEs, tests and docs.
4. Implement an SBase dbt adapter only after lifecycle, catalog, quoting, type mapping, transactions and incremental semantics are validated for the selected engine.
5. Explicitly reject or report unsupported macros, hooks, snapshots and adapter-specific behavior; never silently ignore them.

A dbt Core bridge can be an earlier integration path, but it must run in an isolated, resource-limited worker with pinned dependencies and an allow-listed project directory. Never execute arbitrary project hooks on the application host.

## 4. Materialization and incremental processing

Initial materializations: view, table, incremental and ephemeral.

Incremental strategies:
- Append with deterministic event/ingestion cursor.
- Merge/upsert with declared unique keys.
- Partition overwrite only for explicitly bounded partitions and engines with safe atomic semantics.

Required controls:
- Persist last successful cursor, high-water mark, source watermark and model version.
- Handle equal cursor values with a composite cursor or deterministic tie-breaker; timestamp-only cursors are insufficient when values collide.
- Support lookback windows for late-arriving data with deduplication by declared key.
- Make retries idempotent and persist run/publication state.
- Validate unique keys, nullability and row-count expectations before publication.
- Detect schema drift and require an explicit compatible-change policy.
- Full refresh is separately authorized, confirmed and audited.
- Never destroy/overwrite production targets before staged output passes validation.

Build table materializations in run-scoped staging, validate, then publish. Preserve the previous successful target if the build fails.

## 5. Dependency graph and lineage

### Unified graph

Represent lineage as versioned nodes and directed edges. Node types: source, connector, physical table, table, column, SQL model, model column, pipeline, pipeline step, BI report, report field/metric, dashboard and widget.

Edge types: READS_FROM, DERIVES_FROM, WRITES_TO, CONSUMES, USES_COLUMN, PUBLISHES and DISPLAYS.

Every edge records tenant, evidence source (parser, pipeline definition, connector discovery, user declaration), confidence, observed time, effective version and execution/run ID where applicable. Keep historical snapshots so edits do not erase lineage of prior runs or reports.

### Column-level lineage

- Parse SQL AST and resolve aliases, CTEs, subqueries, joins, expressions and wildcard projections against catalog schema.
- Map each output column to all source columns and transformation expressions.
- Preserve expression text and normalized expression fingerprint.
- Mark dynamic SQL, macros, UDFs and unresolved wildcard expansion as partial/unknown; never fabricate precision.
- Capture lineage from SQL models, SQL pipeline nodes, existing transformation nodes, feature transformations and governed BI queries.
- For non-SQL connectors, use declared mappings and schema discovery where available.

Required navigation in both directions:

source system -> database/schema -> table -> column -> transformation/model -> pipeline run -> BI report/metric -> dashboard/widget

Asset details show upstream/downstream dependencies, owner/team, classification, latest successful run, freshness and quality status.

## 6. Catalog governance

Extend dataset/table/column metadata with:
- Business and technical names, description and data type.
- Data owner (person/service identity) and responsible team.
- Domain, tags and glossary term links.
- Classification: public, internal, confidential, restricted (tenant-configurable).
- Sensitivity flags and retention policy reference.
- Source system/object, connector ID and last discovery timestamp.
- Schema fingerprint, observed schema history and drift events.
- Freshness/quality SLAs and certification state.
- Stewardship state: proposed, verified, certified, deprecated.

Column metadata must be first-class records, not only opaque JSON. Preserve discovered schema separately from curated business metadata.

## 7. Impact analysis

For a table, column, model or pipeline node, return the transitive downstream graph:
- Direct/indirect affected models, pipeline steps, reports, dashboard widgets and scheduled deliveries.
- Dependency path and transformation expression.
- Owner/team, criticality, classification and last successful execution.
- Confidence and unresolved-lineage warnings.
- Estimated blast radius and breaking-change severity.

Compare old and proposed schemas/manifests before deployment:
- Non-breaking: additive nullable column, description/tag change.
- Potentially breaking: type widening/narrowing, rename inferred as drop+add, nullability change.
- Breaking: dropped/renamed referenced column, incompatible type change, removed model or metric.

Block certified production deployments on breaking changes unless an authorized owner approves an exception with reason and audit record. Generate a rollout checklist; do not auto-apply destructive schema changes.

## 8. Tests and documentation generation

Generate model/column docs, source/transformation descriptions, dependency/lineage pages, schema/contract tests, generic tests (not-null, unique, accepted-values, relationships, freshness, row-count bounds), SQL singular tests, documentation coverage and unowned/unclassified asset reports.

Execute tests against isolated schemas or transaction-safe fixtures with deterministic cleanup. Required test failures block publication. Version test definitions and outcomes. Distinguish inferred descriptions from owner-approved business definitions.

## 9. API surface

Proposed endpoints, aligned with existing API conventions:

- POST /api/sql/validate, /api/sql/preview, /api/sql/explain
- GET/POST /api/sql/queries and GET /api/sql/queries/{id}/runs
- GET/POST /api/models, POST /api/models/{id}/validate, POST /api/models/{id}/run, GET /api/models/{id}/runs
- GET /api/catalog/assets and GET /api/catalog/assets/{id}
- PATCH /api/catalog/assets/{id}/ownership
- GET /api/lineage/graph?rootType=...&rootId=...&direction=UPSTREAM|DOWNSTREAM&depth=...
- GET /api/lineage/columns/{id}
- GET /api/lineage/impact?assetId=...
- POST /api/models/{id}/tests/run and GET /api/models/{id}/docs

Every endpoint must use tenant-scoped lookup, resource ACL, pagination/depth limits, audit and rate limiting. Graph APIs must not reveal inaccessible nodes through counts, names, errors or edge paths.

## 10. React workspace

Add five live areas:
1. SQL Studio: editor, schema browser, parameters, validation, explain plan, result grid and history.
2. Model Studio: project/model tree, SQL editor, config, dependency graph, tests, docs and run history.
3. Catalog Explorer: domains, tables, columns, owners, teams, tags, classifications, freshness and quality.
4. Lineage Explorer: interactive upstream/downstream graph across table/column/pipeline/report/dashboard levels.
5. Impact Review: schema diff, affected assets, owners, severity, approval and rollout checklist.

All screens call real APIs and display loading, empty, forbidden, stale-lineage and partial-lineage states. Do not ship static demo graphs as live metadata.

## 11. Data model and migrations

Use additive Flyway migrations. Proposed logical entities:
- catalog_assets, catalog_columns, catalog_tags, catalog_asset_tags, catalog_ownership, catalog_classifications, catalog_schema_versions
- sql_projects, sql_models, sql_model_versions, sql_model_dependencies, sql_model_runs, sql_model_run_artifacts
- sql_query_definitions, sql_query_runs
- lineage_nodes, lineage_edges, lineage_snapshots
- model_tests, model_test_runs, generated_documentation
- impact_approvals

Use UUID primary keys, tenant_id on every tenant-owned row, explicit owner/team identifiers, timestamps, optimistic version columns, foreign keys and indexes for tenant+asset, graph traversal and run history. Unique constraints include tenant scope. Use soft-deprecation for governed assets and immutable model/run snapshots for audit.

Before creating tables, reconcile these concepts with existing datasets, feature_lineage, bi_reports, dashboards, dashboard_widgets, pipelines and pipeline_runs. Bridge existing records instead of creating duplicate catalogs.

## 12. Delivery sequence and acceptance gates

### Phase A — Catalog and graph foundation
- First-class table/column metadata, owner/team, tags and classification.
- Unified graph and ingestion of existing dataset, feature, pipeline, BI report and dashboard relationships.
- Tenant isolation, ACL, graph traversal and impact API.

Gate: integration tests prove cross-tenant graph reads disclose no nodes or edges; existing assets appear with provenance and honest partial-lineage markers.

### Phase B — SQL Studio and SQL pipeline node
- Parser-based dialect validation, typed parameters, read-only execution and resource controls.
- SQL node in DAG validator/executor, run logs, cancellation, retries and idempotent publication.
- React SQL editor connected to live APIs.

Gate: security tests cover stacked statements, DDL/DML, unsafe functions, unauthorized relations, parameter injection, timeout, cancellation and tenant boundaries.

### Phase C — SQL model registry and materializations
- Versioned models, CTEs, ref/source resolution, dependency graph and table/view/ephemeral/incremental materializations.
- Staging-and-publish protocol, composite cursors, late-arriving data and schema drift.
- Model tests and generated docs.

Gate: deterministic reruns, concurrent-run fencing, crash recovery, incremental/full-refresh equivalence and rollback safety.

### Phase D — dbt interoperability
- Manifest import/export and documented supported dbt subset.
- Adapter or isolated dbt Core bridge.
- Compatibility fixture projects.

Gate: no dbt compatibility claim until official fixtures pass compile, run, test, docs and incremental scenarios for each supported engine.

### Phase E — End-to-end governance and impact
- Column lineage from source to dashboard widget.
- Schema/model diff, impact review, approvals and rollout checklist.
- Ownership workflows, freshness/quality SLAs and lineage freshness monitoring.

Gate: seeded end-to-end test traces a dashboard metric to source columns and proves that changing a referenced column identifies every affected report/widget and responsible owner.

## Non-negotiable engineering rules

- Keep the modular monolith.
- No arbitrary SQL through string concatenation or unbounded JDBC.
- No full dbt compatibility claim before conformance tests.
- No fabricated lineage when parser resolution is incomplete.
- No tenant-blind graph traversal or metadata search.
- No destructive incremental publication before staging validation.
- Preserve existing pipeline leases, retry semantics, BI permissions, audit and operations.
- Update English and Turkish READMEs only after the runtime implementation is available.
