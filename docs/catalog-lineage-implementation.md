# Unified Catalog and Lineage — Phase A

## Implementation status

Phase A is implemented as a tenant-scoped catalog and metadata graph foundation. This document describes the runtime implementation; it does not claim that the SQL editor, SQL pipeline executor or dbt runtime are implemented.

The implementation remains in the Spring Boot modular monolith. Catalog metadata and graph persistence use Spring Data JPA/Hibernate; schema changes use additive Flyway migrations. Existing dataset, pipeline, feature-store and BI workflows publish metadata into the same graph.

## Persistence model

### catalog_assets

A shared identity space for data sources, connectors, tables, columns, pipelines, pipeline steps, feature sets, models, reports, report fields, dashboards and widgets.

Every asset is tenant-owned and has:
- Stable UUID, type and tenant-scoped external key.
- Display/physical names and namespace.
- Data owner subject and responsible team.
- Business domain, classification and tags.
- JSONB metadata, lifecycle state and schema fingerprint.
- Optional parent asset, timestamps and optimistic-lock version.

External keys are unique within tenant and asset type. The tenant is part of every lookup and every graph edge foreign key.

### catalog_columns

A normalized, first-class column contract attached to a COLUMN catalog asset:
- Parent table asset and ordinal position.
- Data type and nullability.
- Current/historical marker.

Only current columns participate in active ordinal uniqueness. Retired columns and their graph references remain queryable.

### catalog_schema_versions

Immutable JSONB snapshots keyed by tenant, table and SHA-256 schema fingerprint. Repeated discovery of the same schema is idempotent. The table asset retains the current fingerprint; this table retains prior schema snapshots.

### lineage_edges

Directed, tenant-scoped graph relationships:
- CONTAINS, READS_FROM, DERIVES_FROM, WRITES_TO, CONSUMES, USES_COLUMN, USES_REPORT, DISPLAYS, PUBLISHES and DEPENDS_ON.
- Evidence source, reference, optional transformation expression, confidence, observed time, effective version and optional run ID.
- SHA-256 fingerprint for idempotent writes and version-aware history.

The graph stores evidence rather than pretending all lineage has equal certainty. Migration-derived edges are marked MIGRATION; pipeline edges are PIPELINE_DEFINITION; BI edges are BI_DEFINITION; user-authored declarations are USER_DECLARED. Missing or ambiguous physical table matches are left unresolved rather than guessed.

## Existing workflow synchronization

- Dataset create/update synchronizes the table asset, current column contracts and schema fingerprint in the same database transaction. Removed columns are retired, not deleted. Dataset deletion deprecates the catalog asset while preserving historical lineage.
- Pipeline create/update synchronizes the pipeline asset, step assets, explicit/implicit dependency edges, registered table reads/writes and column usage inferred from the supported pipeline step definitions. Pipeline deletion deprecates its asset.
- Feature lineage APIs and feature materialization publish source table/column and parent-feature relationships when source metadata is available.
- Governed BI report/dashboard/widget create operations synchronize report-to-widget-to-dashboard lineage.
- Flyway backfills pre-existing datasets, pipeline steps, feature sets, BI workspaces, connectors and model registry assets. Malformed legacy JSON is skipped safely; unresolved references remain partial.

The BI dependency direction is upstream-to-downstream for impact traversal: source table/column → report → widget → dashboard. A dashboard-to-widget CONTAINS edge is also retained as a structural relationship.

## API

All routes require the matching permission and resolve tenant identity from the verified JWT tenant claim.

| Route | Purpose |
|---|---|
| GET /api/catalog/assets?type=TABLE&limit=100 | List tenant-visible assets (limit capped at 200) |
| POST /api/catalog/assets | Register a governed catalog asset |
| GET /api/catalog/assets/{id} | Get one tenant-scoped asset |
| PATCH /api/catalog/assets/{id}/governance | Update description, owner/team, domain, classification, tags and lifecycle |
| GET /api/catalog/assets/{id}/columns | List current table columns |
| POST /api/catalog/assets/{id}/columns | Register a column contract |
| GET /api/catalog/assets/{id}/schema-versions | Read immutable schema snapshots (latest 100) |
| POST /api/catalog/lineage/edges | Add an explicitly declared edge |
| GET /api/catalog/lineage/graph?rootId=...&direction=BOTH&depth=4 | Bounded upstream/downstream graph |
| GET /api/catalog/lineage/impact?assetId=...&depth=8 | Bounded downstream impact graph |

Permissions:
- ADMIN and DATA_ENGINEER: catalog read/write and lineage read/write.
- DATA_SCIENTIST and BI_VIEWER: catalog read and lineage read.

Changing ownership to another subject requires tenant-administrator authority. Ordinary governance updates preserve fields omitted from PATCH. Graph traversal and all asset lookups are tenant-scoped; maximum depth is 8, maximum returned nodes is 500 and maximum returned edges is 2,000. The API reports when a result is truncated.

## React workspace

The Catalog & Lineage workspace provides:
- Asset type filtering and text search.
- Owner/team/domain/classification/tags governance editing.
- Table column dictionary and column contract detail.
- Immutable schema version history.
- Interactive selection of graph nodes and edge/evidence inspection.
- Downstream impact traversal.

It uses live catalog APIs; it does not render a static demo graph.

## Flyway migrations

- V21: catalog assets, column contracts, graph edges and legacy dataset/pipeline/BI/feature backfill.
- V22: pipeline-step and legacy BI lineage.
- V23: role permissions and feature/report column lineage.
- V24: connector, model registry and legacy BI source lineage.
- V25: immutable schema-version history.

## Acceptance coverage

PostgreSQL Testcontainers integration tests cover:
- JPA persistence with Flyway and Hibernate schema validation.
- Tenant isolation for asset lookup and edge creation.
- Column registration, duplicate rejection and active schema ordinals.
- Idempotent lineage edge registration.
- Automatic dataset-to-catalog synchronization and column retirement.
- Schema fingerprint history.
- End-to-end source column → pipeline steps → output table → BI report → widget → dashboard impact traversal.

## Deliberate boundaries

- This is the catalog/lineage foundation, not yet a SQL parser or SQL execution service.
- Column lineage is only emitted when the current supported pipeline/BI/feature metadata identifies a source column unambiguously.
- Dynamic SQL, opaque connector transformations and unresolved table names remain partial/unresolved.
- Model/connector backfill provides asset identities and available source relationships; full model feature/column attribution requires runtime metadata.
- The graph API is bounded and returns evidence/provenance; it is not yet a semantic business glossary or automated schema-change approval workflow.
- dbt support, SQL models, incremental materialization, SQL pipeline nodes and parser-based SQL lineage remain later phases.
