# Advanced lakehouse operations

This change set builds on the Iceberg JDBC catalog and S3-compatible object storage. It adds Java services and durable metadata contracts for the operational features below.

## Authorization

`LakehouseControlPlane.requireNamespace` and `requireTable` are authorization gates for catalog adapters. They enforce workspace ownership, namespace USE, direct-user/workspace/service/group grants, expiry and revocation. Table grants support SELECT/INSERT/UPDATE/DELETE/ALTER/DROP/OWNERSHIP; namespace grants provide inherited fallback privileges. The adapter that opens an Iceberg Catalog must call these methods before every metadata or data operation. Never expose the raw Iceberg catalog directly to an untrusted caller.

Grant writes are audit-preserving upserts/revocations. Group membership must come from the authenticated identity provider; clients must not be allowed to self-assert group IDs.

## Lineage

`SparkLineageListener.install` registers a Spark QueryExecutionListener and persists successful query table dependencies. It uses analyzed logical-plan relation nodes, rather than parsing arbitrary SQL text. Spark plan rendering varies by data source and Spark release; unsupported nodes should be treated as unknown lineage, not as proof that a table was not read. The listener currently records table-level edges only; column-level lineage and write-target extraction require plan-specific adapters and integration fixtures for each supported Spark version.

## Ingestion

`LakehouseIngestionService` supplies batch readers for:
- S3/S3A Parquet, ORC, JSON, CSV and Avro objects
- JDBC tables/queries, with credentials resolved through a SecretResolver
- Kafka batch topics and offsets

It writes DataFrames into Iceberg tables through Spark's DataFrameWriterV2. Pipeline definitions are versioned in PostgreSQL; ingestion run, checkpoint/offset, retry and dead-letter metadata are persisted separately. The caller must validate the workspace and source/table grants before invoking these adapters.

## Flink streaming

`FlinkIcebergStreamingJob` enables EXACTLY_ONCE checkpoints and creates an Iceberg JDBC catalog backed by S3FileIO. It accepts an INSERT INTO statement to a registered Iceberg table. The Iceberg Flink runtime and the selected source connector (Kafka, CDC, etc.) must be present on the Flink cluster classpath. Checkpoint storage must be durable and shared by all JobManagers/TaskManagers. Production deployments must also configure checkpoint retention, restart strategy, and monitor failed/expired checkpoints.

The CDC offset repository uses transactional compare-and-set generations so concurrent consumers cannot overwrite a newer committed offset. Dead-letter records are claimed with `FOR UPDATE SKIP LOCKED` and a five-minute lease token. Expired claims can be recovered; stale workers are fenced from resolving a record after a newer worker has reclaimed it. Claims are capped at ten attempts. The Lakehouse Studio provides manual inspection, claim, resolve and requeue controls; it does not yet re-run the original source connector automatically.

## Schema evolution

Spark ingestion supports additive columns, column rename and a whitelist of widening types through Iceberg ALTER TABLE statements. Destructive schema changes are deliberately not auto-applied. Migration history is stored in `lakehouse_schema_change`; callers should record each successful schema change and reject breaking changes unless explicitly approved.

## Maintenance

`IcebergMaintenanceService` exposes:
- `rewrite_data_files` compaction with bounded target file size
- `rewrite_manifests`
- `expire_snapshots` with a retain-last floor and age threshold
- `remove_orphan_files` with a minimum 24-hour safety age

V15/V16 store maintenance policies, run history, due times and leases. A distributed scheduler must claim due policies using the lease/fencing-token tables before executing a procedure. Do not run orphan cleanup concurrently with active writers unless the age threshold is safely larger than the maximum commit/retry window.

## Current integration boundary

The authenticated API is mounted from `Main.scala` and now exposes namespace/table catalog operations, grant management, lineage, schema evolution, snapshots, maintenance policies/history, ingestion pipelines/runs, CDC offsets and dead-letter recovery. The server starts the distributed pipeline dispatcher and maintenance scheduler. The Spark lineage listener is registered at startup and reads the workspace/principal from the request or job execution context.

The React Lakehouse Studio now includes Catalog, Pipeline, Governance/Lineage, Maintenance and Dead-letter workspaces. Flink Kafka connector, JSON, Iceberg runtime, AWS bundle, PostgreSQL JDBC and S3 filesystem artifacts are prebundled into the Flink image with checksum verification.

Remaining production-hardening work:
1. Replace the best-effort Spark plan-string target extraction with Spark-version-specific logical-plan visitors and integration fixtures; lineage remains table-level, not column-level.
2. Add automated source replay that reprocesses claimed dead-letter payloads through the original connector/transform pipeline. The current UI supports manual review, resolve and requeue only.
3. Run the full end-to-end tenant-isolation suite across every route, Spark/Flink execution path, scheduler and connector, including concurrent grant revocation and job lease expiry.
4. Run a real Kafka → Flink checkpoint → Iceberg commit test against the full Compose stack; CI currently validates compilation and PostgreSQL behavior but does not prove cluster-level exactly-once recovery.
5. Add schema-compatibility policy enforcement and automatic schema-change audit entries to every ingestion/evolution path.
