# SBase Lakehouse Architecture

SBase's lakehouse uses Apache Iceberg tables stored as Parquet data and Iceberg metadata files in an S3-compatible object store. PostgreSQL is the shared metadata database for the Iceberg JDBC catalog; Spark is the primary batch/SQL execution engine.

## Architecture and ownership

| Layer | Component | Responsibility |
|---|---|---|
| Object storage | MinIO (S3 API) | Durable table data, manifests, metadata JSON and snapshots |
| Table format | Apache Iceberg 1.10 | ACID commits, schema evolution, partition metadata, snapshot history and maintenance |
| Shared metadata catalog | Iceberg JDBC Catalog backed by PostgreSQL | Namespace/table registration, table locations and current metadata pointers |
| Compute | Apache Spark 4 | SQL, batch ETL, table writes, previews, compaction and snapshot maintenance |
| Control plane | SBase API + PostgreSQL | Workspaces, jobs, workflow orchestration and API access |
| Streaming | Apache Flink 2 | Separate streaming runtime; Iceberg sink integration is a follow-on stage |

The Iceberg JDBC catalog is the authoritative catalog for Iceberg namespaces and tables. Iceberg creates and manages its own catalog metadata tables in the configured PostgreSQL database. SBase does not maintain a second, competing list of Iceberg tables.

## Start the full lakehouse stack

Create or edit `.env`. Set strong, unique passwords before exposing any ports. The full Compose overlay supplies these defaults for local development:

```dotenv
SBASE_ICEBERG_ENABLED=true
SBASE_ICEBERG_WAREHOUSE=s3://sbase-warehouse/
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=replace-with-a-long-minio-password
S3_REGION=us-east-1
S3_PATH_STYLE_ACCESS=true
```

Start the stack from the repository root:

```bash
docker compose -f compose.yaml -f compose.full.yaml --profile full up --build -d
```

MinIO's data is persisted in the `minio_data` volume. The `minio-init` one-shot service creates the `sbase-warehouse` bucket and enables bucket versioning. The application waits for PostgreSQL health and successful bucket initialization. MinIO liveness is checked at `/minio/health/live`.

The local MinIO console is exposed on port 9001 and the S3 API on port 9000 by default. These are development defaults, not a production security configuration.

## API

All endpoints are under `/api/v1`.

| Method | Path | Purpose |
|---|---|---|
| GET | `/lakehouse` | Iceberg catalog and S3 endpoint health |
| GET | `/lakehouse/namespaces` | List namespaces |
| POST | `/lakehouse/namespaces` | Create a namespace; JSON: `{"namespace":"analytics"}` |
| GET | `/lakehouse/namespaces/{namespace}/tables` | List Iceberg tables |
| GET | `/lakehouse/namespaces/{namespace}/tables/{table}` | Bounded table preview; optional `?limit=100` |
| GET | `/lakehouse/namespaces/{namespace}/tables/{table}/schema` | Schema and table DDL/partitioning |
| POST | `/lakehouse/namespaces/{namespace}/tables/{table}/write` | Append or create from bounded JSON rows |
| GET | `/lakehouse/namespaces/{namespace}/tables/{table}/snapshots` | Snapshot history |
| POST | `/lakehouse/namespaces/{namespace}/tables/{table}/snapshots/{snapshotId}/rollback` | Roll back current table pointer |
| POST | `/lakehouse/namespaces/{namespace}/tables/{table}/snapshots/expire` | Expire eligible old snapshots |
| POST | `/lakehouse/namespaces/{namespace}/tables/{table}/compact` | Rewrite small data files |

Example:

```bash
curl -X POST http://localhost:8080/api/v1/lakehouse/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"analytics"}'

curl -X POST http://localhost:8080/api/v1/lakehouse/namespaces/analytics/tables/events/write \
  -H 'Content-Type: application/json' \
  -d '{"rows":[{"event_id":1,"region":"EU","amount":12.5},{"event_id":2,"region":"US","amount":20.0}]}'

curl -X POST http://localhost:8080/api/v1/engines/spark/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT region, SUM(amount) AS total FROM sbase.analytics.events GROUP BY region","limit":100}'
```

Table write API accepts at most 10,000 JSON rows per request and is intended for small uploads and previews. Use Spark jobs for large ingestion. Table and namespace identifiers are validated before being interpolated into Spark SQL.

### Snapshot recovery and maintenance

Use snapshot history to obtain a snapshot ID, then call the rollback endpoint with that ID. Snapshot expiration requires an ISO-8601 UTC `olderThan` value and `retainLast` from 1 to 1000. Expiration is destructive: it permanently removes files no longer referenced by retained snapshots. Compaction rewrites data files and should be scheduled according to observed file sizes and query patterns, not on every write.

## Spark SQL

The Spark session registers the `sbase` Iceberg catalog with:

- `type=jdbc` and the PostgreSQL JDBC URL/credentials for shared catalog metadata.
- `warehouse=s3://sbase-warehouse/` for table data and Iceberg metadata.
- Iceberg `S3FileIO` with endpoint, region, path-style access and S3 credentials.
- Iceberg Spark SQL extensions for Iceberg procedures and table operations.

The runtime includes the Iceberg Spark runtime, AWS bundle and JDBC catalog module. The JDBC driver is available to the Spark driver through the application runtime. Do not store credentials in notebook SQL, table properties or workflow definitions.

## Operational and production boundaries

- The full Compose profile is a single-host development topology. MinIO is not distributed or highly available in this configuration.
- Replace default credentials, restrict MinIO/PostgreSQL/Spark/Flink ports to private networks, enable TLS and use a secrets manager before production.
- Back up PostgreSQL catalog metadata and object storage together. Restoring only one side can leave catalog pointers and data files inconsistent.
- Enable object-store replication/versioning and test restore procedures. Bucket versioning is enabled by the local bootstrap container, but it is not a substitute for backup.
- Configure Spark event logs, worker capacity, query quotas and per-workspace resource limits before multi-user production.
- Namespace/table identifier validation is present, but full row/column governance, tenant-scoped catalog authorization and audited grants are still separate platform work.
- Flink is deployed as a compute engine, but a production-grade Flink-to-Iceberg streaming sink with checkpointed exactly-once commits is not claimed by this release.

## Next lakehouse stages

1. Add catalog authorization, ownership, tags, grants and lineage integrated with workspace boundaries.
2. Add scalable file ingestion from S3, JDBC, Kafka and CDC with schema evolution and dead-letter handling.
3. Add Flink Iceberg sinks with checkpoint/recovery integration and replay tests.
4. Add table maintenance scheduling, orphan-file cleanup policies, manifest rewrite and usage/cost metrics.
5. Add query history, SQL workspace sessions, notebook execution and per-workspace compute profiles.
