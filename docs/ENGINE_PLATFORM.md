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

All vector operators require numeric arrays; pairwise operations require equal, non-empty dimensions. Top-K sorts by descending value and uses ascending coordinate index as a stable tie-breaker.

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
