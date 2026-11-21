# Lakehouse runtime integration

This branch connects the Iceberg catalog control plane to the HTTP API, Spark execution,
Flink submission and the React Lakehouse Studio. The feature is intentionally isolated
from `main` until the backend and full Compose CI jobs pass.

## Authentication and tenant identity

The Lakehouse API and Spark SQL workspace require a signed HS256 JWT:

- `Authorization: Bearer <token>`
- required claims: `sub`, `workspace_id`, `exp`
- optional claims: `nbf`, `groups` (string array), `service_id`
- signing key: `SBASE_JWT_SECRET` (minimum 32 characters)

The workspace and principal are taken from the verified token, never from request
headers or JSON fields. Set a unique random secret in production; the Compose default
is only for local development/CI. The React Studio stores the token in tab-scoped
sessionStorage.

Catalog access is checked against namespace ownership/grants and table grants before
catalog metadata, previews, writes, schema changes, SQL reads, maintenance operations,
pipeline creation, lineage and CDC operations. Namespace ownership and grant expiry,
revocation, user/group/workspace/service principals are evaluated in PostgreSQL.

## HTTP surface

All routes below are under `/api/v1` and require the bearer JWT.

| Method | Route | Purpose |
|---|---|---|
| GET/POST | `/lakehouse/namespaces` | List/create workspace namespaces |
| GET | `/lakehouse/namespaces/{ns}/tables` | Authorized table listing |
| GET | `/lakehouse/namespaces/{ns}/tables/{table}` | Bounded table preview |
| POST | `/lakehouse/namespaces/{ns}/tables/{table}/write` | Bounded JSON write |
| GET | `/lakehouse/namespaces/{ns}/tables/{table}/schema` | Schema inspection |
| POST | `/lakehouse/namespaces/{ns}/tables/{table}/schema/evolve` | Add/rename/widen schema |
| GET | `/lakehouse/namespaces/{ns}/tables/{table}/snapshots` | Snapshot history |
| POST | `/lakehouse/namespaces/{ns}/tables/{table}/compact` | Authorized compaction |
| GET/POST | `/lakehouse/namespaces/{ns}/grants` | List/grant namespace privileges |
| POST | `/lakehouse/namespaces/{ns}/grants/revoke` | Revoke namespace privilege |
| POST | `/lakehouse/namespaces/{ns}/tables/{table}/grants` | Grant table privilege |
| POST | `/lakehouse/namespaces/{ns}/tables/{table}/grants/revoke` | Revoke table privilege |
| GET | `/lakehouse/namespaces/{ns}/lineage` | Workspace lineage edges |
| GET/POST | `/lakehouse/pipelines` | List/create versioned pipelines |
| GET/POST | `/lakehouse/pipelines/{id}/runs` | Inspect/queue idempotent runs |
| POST | `/lakehouse/sources/{id}/cdc/offsets` | Compare-and-set CDC offset |
| GET/POST | `/lakehouse/runs/{id}/dead-letter` | Inspect/claim replay records |
| POST | `/lakehouse/dead-letter/{id}/finish` | Mark replay resolved or retryable |
| POST | `/engines/spark/sql` | Read-only, tenant-authorized SQL |

Schema evolution supports `ADD_COLUMN` with a safe column/type allow-list,
`RENAME_COLUMN` and `TYPE_WIDEN`. Destructive schema operations are deliberately
not exposed through this endpoint.

The SQL workspace accepts read-only SELECT/WITH statements and requires every
physical table reference to be fully qualified as `sbase.namespace.table` (CTEs
may be used). DML, DDL, procedure calls and other catalog mutation statements are
rejected. Spark lineage is captured by a registered QueryExecutionListener and
is table-level/best-effort; it is not column-level lineage.

## Ingestion pipelines and dispatch

Pipeline definitions are versioned and stored in PostgreSQL. Run submission is
idempotent by `(pipeline_id, idempotency_key)`. A multi-instance dispatcher claims
attempts using `FOR UPDATE SKIP LOCKED`, records a lease owner and fencing token,
and executes Spark batch or Flink streaming work.

Example S3/Spark pipeline:

```json
{
  "name": "daily-orders",
  "sourceType": "S3",
  "source": {"uri": "s3a://sbase-warehouse/incoming/orders/", "format": "parquet"},
  "targetNamespace": "analytics",
  "targetTable": "orders",
  "engine": "SPARK",
  "mode": "BATCH",
  "configuration": {},
  "maxRetries": 5
}
```

Supported Spark batch sources are S3-compatible object storage (Parquet, ORC, JSON,
CSV, Avro), JDBC and Kafka batch reads. JDBC passwords are resolved at execution time
from `SBASE_SECRET_<NORMALIZED_SECRET_REF>`; the reference, not the secret, is stored
in PostgreSQL. Spark batch writes are at-least-once: a worker crash after Iceberg commit
but before acknowledgement can cause a retry to append the batch again. Use a stable
source batch identifier/deduplication strategy for business-level exactly-once.

Flink streaming/CDC pipelines require `configuration.flinkJarId`, `sourceDdl` and
`insertSql`, plus a durable `checkpointUri`. The submitted entry class is
`io.sbase.server.lakehouse.FlinkIcebergStreamingJob`; it enables EXACTLY_ONCE
checkpoints and uses the Iceberg JDBC catalog with S3FileIO. The returned run remains
RUNNING after successful submission because it represents a long-lived stream.

### Flink connector runtime

The Compose profile builds `flink/Dockerfile`. Put the Kafka SQL connector and any
other source connector JARs in `flink/lib/`, then rebuild/restart the Flink cluster:

```bash
docker compose -f compose.yaml -f compose.full.yaml --profile full up --build -d flink-jobmanager flink-taskmanager
```

Upload the SBase all-in-one JAR (the `*-all.jar` from `gradle shadowJar`) through
the Flink Web UI JAR upload page, then use the returned 32-character JAR ID in the
pipeline configuration. Connector JARs must be present on every JobManager and
TaskManager node before submission. The application JAR is not uploaded automatically
by the pipeline API.

## Distributed maintenance

The maintenance scheduler runs in every app instance. PostgreSQL row locks with
SKIP LOCKED elect one owner per due policy; lease owner plus fencing token prevents
a stale scheduler from recording completion after another owner has taken over.
Policies use bounded `intervalSeconds` scheduling (minimum 60 seconds), with
compaction, manifest rewrite, snapshot expiration and orphan cleanup. Orphan cleanup
requires an age of at least 24 hours. Runs and scheduler events are persisted.

## CDC and dead-letter handling

CDC offsets are committed with a generation compare-and-set to prevent concurrent
writers from moving the same checkpoint backwards. Dead-letter claim uses SKIP LOCKED,
so only one replay worker can claim a record at a time. A replay consumer claims a
batch, reprocesses the payload against its source/target contract, then calls finish
with `resolved=true`; failed replay can return the record to PENDING with
`resolved=false`. This API provides durable claim/state coordination; source-specific
payload replay remains the responsibility of the connector worker.

## Validation

The CI backend job compiles Java/Scala and runs PostgreSQL integration tests, including
group grants, table grants, expiry, revocation and cross-workspace denial. The full
Compose job verifies JWT rejection without a token, authenticated namespace creation,
Iceberg writes/reads/snapshots, Spark worker registration and Flink TaskManager
registration. The Flink connector execution path additionally requires connector JARs
to be supplied in `flink/lib/`; CI does not fabricate third-party connector binaries.
