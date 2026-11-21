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

The CDC offset repository uses transactional compare-and-set generations so concurrent consumers cannot overwrite a newer committed offset. Dead-letter records are claimed with `FOR UPDATE SKIP LOCKED`, allowing multiple replay workers without double-claiming a record.

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

The service classes and database contracts are now present, but this is not yet a claim that every operation is wired into public HTTP routes or that production multi-tenant isolation has passed end-to-end tests. Before enabling a public endpoint, the next integration work is:
1. Construct the control plane from the server's existing authenticated identity and DataSource.
2. Put mandatory authorization gates around every Spark, Flink and Iceberg catalog entry point.
3. Register the Spark listener with authenticated workspace/job context.
4. Persist ingestion pipeline submissions and dispatch them to the Spark/Flink job managers.
5. Run the Flink connector integration test with a real Kafka source, checkpoint storage and Iceberg JDBC catalog.
6. Add cross-workspace denial, expired/revoked grant, CDC race/replay, schema evolution and maintenance lease tests.
