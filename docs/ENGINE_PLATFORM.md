# SBase engine platform

SBase is evolving toward a unified lakehouse and AI workspace. The control plane remains PostgreSQL + Pekko; compute engines are execution backends selected per workload. Do not treat the engines as interchangeable.

## Engine responsibilities

| Engine | Platform responsibility | Current integration |
|---|---|---|
| Apache Spark 4 | Batch ETL, SQL analytics, distributed joins, Spark ML and structured-streaming query lifecycle | Spark SQL preview, logical/physical plan inspection, catalog table discovery/cache, active stream listing/stop, AutoML and federated joins |
| Apache Flink 2 | Stateful, low-latency stream processing and event-time pipelines | REST job submission, overview/details, cancellation and savepoint requests |
| Apache Mahout | Vector math and selected scientific mining operators | Vector sum, cosine similarity, Euclidean distance, dot product, Manhattan distance, L2 normalization and top-K coordinates |
| Apache Cassandra | High-write, partition-oriented operational/feature data | Keyspace discovery, table/view listing, column/primary-key metadata and bounded read-only CQL |
| Apache Calcite | SQL parsing, validation and federated planning | Read-only SQL validation and federated planning |
| Apache Pekko | Durable orchestration and distributed worker coordination | Versioned DAGs, leases, fencing, retries, sharding and scheduling |

PostgreSQL remains the authoritative control-plane database. Cassandra is not used for workflow metadata. Solr is a derived search index, not the system of record.

## Engine APIs

All paths are under `/api/v1`.

### Spark

- `GET /engines` — runtime information.
- `POST /engines/spark/sql` — bounded read-only SQL result preview.
- `POST /engines/spark/explain` — optimized and physical query plans without collecting result rows.
- `GET /engines/spark/tables?database=default` — Spark session catalog tables.
- `POST /engines/spark/cache` with `{"table":"analytics.events"}` — cache a validated table identifier.
- `GET /engines/spark/streams` — active Structured Streaming query status and latest progress.
- `POST /engines/spark/streams/{queryId}/stop` — stop one active query.

Spark SQL result previews are deliberately capped. Use distributed Spark jobs for large outputs; do not route large datasets through the API response.

### Flink

- `GET /engines/flink/overview`, `/jobs`, `/jars`, `/taskmanagers`.
- `GET /engines/flink/jobs/{jobId}` — job details.
- `POST /engines/flink/jars/{jarId}/run` — submit an uploaded JAR.
- `POST /engines/flink/jobs/{jobId}/cancel` — request cancellation.
- `POST /engines/flink/jobs/{jobId}/savepoints` with `{"targetDirectory":"s3://bucket/flink/savepoints"}` — request a savepoint.

Flink must be configured with durable checkpoint/savepoint storage and a reachable REST endpoint. The API requests a savepoint; it does not provision or validate the underlying object-store permissions.

### Cassandra

- `GET /catalogs` — source availability and keyspaces.
- `GET /catalogs/cassandra/keyspaces/{keyspace}/tables` — tables and materialized views.
- `GET /catalogs/cassandra/keyspaces/{keyspace}/tables/{table}` — column types and partition/clustering key membership.
- `POST /catalogs/cassandra/query` — bounded, read-only CQL.

Model Cassandra tables around known partition access patterns. Avoid full-table scans and do not use `ALLOW FILTERING` as a substitute for a suitable primary key.

### Mahout workflow operators

The operator catalog is discoverable through `GET /operators/catalog`. New operators:

- `mahout-dot-product`: `left`, `right`.
- `mahout-manhattan-distance`: `left`, `right`.
- `mahout-normalize`: `values`.
- `mahout-top-k`: `values`, `k`.
- `dataset-rows`: emits JSON records; connect it to a `spark-sql` node and set `inputTable` to register those records as a temporary Spark SQL view.
- `cassandra-query`: `cql`, optional `limit`; executes bounded read-only CQL through the configured Cassandra catalog and emits rows that can feed a downstream Spark SQL node.

All vector operators require numeric arrays; pairwise operations require equal, non-empty dimensions. Top-K sorts by descending value and uses ascending coordinate index as a stable tie-breaker.

A Cassandra-to-Spark workflow can connect a `cassandra-query` node to a `spark-sql` node with `inputTable` set (for example, `orders`). Spark registers the bounded upstream row set as a temporary view before executing the query. This is intended for bounded operational extracts and previews, not bulk replication of an entire Cassandra keyspace.

## Databricks-style target architecture

The next stages should be delivered incrementally, with each stage having integration tests and operational telemetry:

1. **Lakehouse storage and table format** — introduce S3-compatible object storage and Apache Iceberg (or one explicitly selected open table format) with catalog-backed tables, snapshots, schema evolution, partition evolution, compaction and time travel. Avoid maintaining competing Delta/Iceberg/Hudi formats until there is a concrete interoperability requirement.
2. **Unified catalog and governance** — extend the current catalog with namespaces, managed/external tables, ownership, grants, row/column policies, tags, lineage, data quality expectations and audit events. Tenant and workspace boundaries must be enforced in every catalog lookup and query plan.
3. **Notebooks and SQL workspace** — persistent notebooks, SQL editor, version history, parameterized execution, shared compute sessions, query history, result pagination and downloadable artifacts.
4. **Job compute profiles** — named Spark batch, Spark streaming and Flink profiles with per-workspace CPU/memory/parallelism quotas, queueing, cancellation, retry policy, autoscaling hooks and fair scheduling.
5. **Streaming and CDC** — Kafka Connect/Debezium ingestion, schema registry integration, Flink event-time windows, checkpoint recovery, dead-letter routing and replayable offsets.
6. **ML lifecycle** — feature registry/store, experiment tracking, reproducible training environments, model registry, online/batch serving, drift and promotion gates.
7. **Observability and cost controls** — OpenTelemetry traces/metrics, Spark event logs/history server, Flink checkpoint metrics, per-workspace usage metering, query/job cost estimates and alerts.

## Production boundaries

The current Compose deployment is a development topology, not a highly available production cluster. Before production: secure engine REST endpoints on a private network, configure TLS and secrets, persist Spark event logs and Flink checkpoints/savepoints, use shared object storage for artifacts, enforce workspace/tenant isolation, add per-engine resource quotas, and run backup/restore plus failure-injection tests.

## Apache Iceberg lakehouse

The full Compose profile now includes MinIO as an S3-compatible object store and bootstraps the `sbase-warehouse` bucket. Spark uses the Apache Iceberg Spark 4.0 runtime, S3FileIO, and a shared JDBC catalog backed by the existing PostgreSQL control-plane database. Table data and Iceberg metadata files live in the object store; catalog/namespace/table registration is shared through PostgreSQL.

### Lakehouse APIs

- `GET /lakehouse` — catalog, warehouse and namespace status (never returns credentials).
- `GET /lakehouse/namespaces` — list namespaces.
- `POST /lakehouse/namespaces` with `{"namespace":"analytics"}` — create a namespace.
- `GET /lakehouse/namespaces/{namespace}/tables` — list tables.
- `POST /lakehouse/namespaces/{namespace}/tables/{table}/write` with `{"rows":[{"id":1,"region":"EU"}]}` — create/replace an Iceberg table from a bounded JSON batch (maximum 10,000 rows).
- `GET /lakehouse/namespaces/{namespace}/tables/{table}?limit=100` — preview rows.
- `GET /lakehouse/namespaces/{namespace}/tables/{table}/snapshots` — inspect recent Iceberg snapshots.

Example flow:

```bash
curl -X POST http://localhost:8080/api/v1/lakehouse/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"analytics"}'

curl -X POST http://localhost:8080/api/v1/lakehouse/namespaces/analytics/tables/orders/write \
  -H 'Content-Type: application/json' \
  -d '{"rows":[{"order_id":1,"region":"EU","amount":120.5},{"order_id":2,"region":"US","amount":80.0}]}'

curl 'http://localhost:8080/api/v1/lakehouse/namespaces/analytics/tables/orders?limit=100'
curl http://localhost:8080/api/v1/lakehouse/namespaces/analytics/tables/orders/snapshots
```

The REST row writer is intentionally for previews and small batches. Production ingestion should use Spark jobs writing directly to Iceberg so large datasets are not serialized through the API. Use a non-default MinIO root password in `.env`; the example credentials are development-only. The JDBC catalog shares the existing SBase PostgreSQL database but owns its Iceberg catalog tables separately from SBase workflow tables.

### Storage and governance boundary

PostgreSQL is the shared metadata catalog/control plane; MinIO stores Iceberg data, manifests and metadata files. This first stage provides namespace/table discovery, writes, previews and snapshot history. It does not yet provide full Unity-Catalog-style grants, row/column policies, schema/partition evolution workflows, compaction scheduling, retention policies, or cross-tenant authorization. Those must be implemented before exposing the lakehouse to untrusted tenants.\n## Lakehouse storage and catalog\n\nThe full Compose profile provisions MinIO, creates the `sbase-warehouse` bucket and configures Spark with Apache Iceberg `S3FileIO` plus the PostgreSQL-backed JDBC catalog. Namespace/table, schema, bounded write, preview, snapshot rollback, compaction and snapshot-expiration APIs are documented in [Lakehouse Architecture](LAKEHOUSE_ARCHITECTURE.md).\n