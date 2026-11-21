# Lakehouse catalog governance

This stage adds the PostgreSQL control-plane schema for workspace-scoped governance around the shared Apache Iceberg JDBC catalog.

## Source-of-truth boundary

- Iceberg JDBC Catalog in PostgreSQL owns Iceberg namespace/table registration and current metadata pointers.
- S3-compatible object storage owns Parquet data files, Iceberg metadata JSON, manifests and snapshots.
- SBase control-plane tables own workspace ownership, grants, tags, table lineage, connector definitions and ingestion-run state.
- SBase governance metadata is an authorization overlay; it must not become a second catalog of physical Iceberg tables.

## Migration

Flyway migration `V14__lakehouse_catalog_governance.sql` creates:

| Table | Purpose |
|---|---|
| `lakehouse_namespace_owner` | Maps an Iceberg namespace to its SBase workspace and owner principal |
| `lakehouse_namespace_grant` | Namespace privileges for users, groups, workspaces and services |
| `lakehouse_table_tag` | PII, retention, domain, quality and certification tags |
| `lakehouse_lineage_edge` | Table-level source/target lineage, linked to workflow job runs |
| `lakehouse_data_source` | Workspace-scoped S3/JDBC/Kafka/Cassandra/Solr connector metadata |
| `lakehouse_ingestion_run` | Durable Spark/Flink ingestion status, row counts and checkpoint/offset positions |

Connector credentials are never stored in `configuration`; `secret_ref` is an external secret-manager reference.

## Privilege model

Supported namespace privileges are `USE`, `CREATE`, `SELECT`, `MODIFY`, `DROP` and `OWNERSHIP`. Grants can be time-bounded and revoked without deleting the audit record. Principal types are `USER`, `GROUP`, `WORKSPACE` and `SERVICE`.

**Important:** V14 establishes persistence and constraints only. The existing Iceberg SQL and REST endpoints are not yet wired to enforce these grants. Do not expose this deployment as multi-tenant-secure until the authorization service and negative cross-workspace tests are implemented. The next stage must apply grants before every catalog operation and must record lineage/ingestion transitions in the same control-plane transaction as their state changes.

## Operational expectations

- Keep PostgreSQL catalog metadata and object-store data backed up as one recovery unit.
- Do not delete S3 objects directly while Iceberg snapshots may reference them.
- Use Iceberg procedures for snapshot expiration, orphan-file cleanup and compaction.
- Keep large ingestion off the bounded JSON write endpoint; use Spark batch or checkpointed Flink streaming jobs.
- Persist connector secrets outside PostgreSQL and redact them from API responses and logs.
