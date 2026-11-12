# Tenant, HA and operational controls

## Tenant identity, membership and ownership

Tenant-scoped APIs require a UUID `tenant_id` (or `tenant`) claim in the verified JWT and an ACTIVE `tenant_memberships` row. Tenant IDs are never accepted from request bodies or query parameters. Creating a tenant automatically adds its creator as OWNER. Platform administrators can add members; tenant owners/admins can grant resource access. Membership roles are OWNER, ADMIN, MEMBER and READ_ONLY; READ_ONLY members cannot perform write operations.

Migration V13 backfills existing datasets, pipelines and pipeline runs into the reserved `legacy-default` tenant. Migration V14 adds tenant ownership to mining jobs, ingestion connectors/runs/records/checkpoints/DLQ, experiments, models/artifacts, feature sets/materializations/lineage, schedules, dashboards, BI reports and child records. Parent-child triggers propagate tenant IDs to worker-created records. V15 adds explicit membership/invitation state.

Datasets and pipelines carry `tenant_id` and `owner_subject`; their CRUD and list paths are tenant scoped. Dataset/pipeline ACL grants support READ, WRITE and ADMIN. Grant management is available at `/api/access-grants` and requires tenant OWNER/ADMIN membership.

Example grant:

```http
POST /api/access-grants
Authorization: Bearer <tenant-scoped-token>
Content-Type: application/json

{"resourceType":"DATASET","resourceId":"<uuid>","subject":"analyst@example.com","permission":"READ"}
```

Mining job records, ingestion execution/idempotency, DLQ list/replay, feature-set access, online feature reads/writes and pipeline runs now carry or validate tenant scope. S3 artifact helper methods support tenant-prefixed keys under `tenants/<tenant-uuid>/`.

Pipeline Spark reads now require a physical `tenant_id` column and apply the tenant from the immutable pipeline-run row before transformations. Pipeline writes reject missing or foreign tenant IDs instead of silently filtering bad records. Governed BI aggregate and drill-through queries also require and predicate on `tenant_id`. This is defense in depth, not a claim of complete isolation: ML lifecycle source scans, legacy raw Doris query paths, every dashboard/model/artifact endpoint and all trusted workers still require end-to-end verification. Do not expose a shared multi-tenant service until cross-tenant integration tests cover every endpoint, Spark/Doris query path, schedule, model, artifact and connector registration.

## Audit and API throttling

Audit events currently capture API request outcome, subject, tenant claim, request ID and peer metadata. This is request-level operational evidence; high-value data mutations and worker lifecycle transitions should also emit domain audit events before regulated production use.

Every `/api/**` request is recorded in `platform_audit_events` with subject, tenant claim, route, method, outcome, status, request ID, peer IP and user agent. The API `GET /api/platform/audit-events` exposes tenant-scoped, paginated events to subjects with `audit:read`. Audit records are retained for 180 days by default (configurable with `sbase.audit.retention-days`).

A PostgreSQL-backed atomic token bucket runs after bearer authentication. Authenticated keys use subject and normalized route; unauthenticated requests use the socket peer IP. The reverse-proxy forwarded IP headers are not trusted. For high-throughput multi-replica deployments, move rate-limit state to Redis and partition/retain audit events according to compliance requirements.

## Doris high availability

Use the additional topology overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml -f docker-compose.doris-ha.yml config
docker compose -f docker-compose.yml -f docker-compose.production.yml -f docker-compose.doris-ha.yml up -d
```

It defines three FE nodes and three BE nodes with persistent volumes, registers the three backends and configures default replication factor 3. SBase uses a MySQL Connector/J load-balanced URL across the three FEs. Place FE/BE nodes on separate hosts/AZs for actual host-failure tolerance. Verify `SHOW FRONTENDS`, `SHOW BACKENDS`, tablet replica health and replication factor after bootstrap. Migrate existing tables deliberately. The base one-FE/one-BE profile remains for CI and local development. Validate this topology against the exact Doris image startup scripts and test FE/BE failover before production.

## Doris snapshot / restore

Scripts `ops/backup-doris.sh` and `ops/restore-doris.sh` use Doris S3 repositories. Provide a dedicated least-privilege S3 identity and endpoint. The scripts accept an explicit comma-separated table list and stable snapshot label. A successful SQL submission is not proof of a completed backup: check `SHOW BACKUP FROM analytics`, record its exact backup timestamp, copy repository objects off the failure domain and perform restore drills. Restore is destructive and requires `CONFIRM_DORIS_RESTORE=YES` plus `DORIS_BACKUP_TIMESTAMP` copied from `SHOW BACKUP`.

## S3 / MinIO object lifecycle

Start the optional MinIO overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml -f docker-compose.object-storage.yml up -d
```

It creates `sbase-artifacts` and `doris-backups`, enables versioning and expires current objects after 365 days and non-current versions after 90 days. Set `SBASE_S3_ENDPOINT=http://minio:9000`, `SBASE_S3_ACCESS_KEY`, `SBASE_S3_SECRET_KEY` and `SBASE_S3_BUCKET=sbase-artifacts` for the application. S3 helper APIs can scope object keys by tenant. A single MinIO instance is not HA; use distributed MinIO or external S3 with replication, encryption, object lock where required and off-site backup. Align lifecycle expiry with legal retention and artifact references.

## Kafka HA and schema evolution

Start the Kafka KRaft overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka-ha.yml up -d
```

Three broker/controller nodes use replication factor 3 and min ISR 2 for internal topics. Schema Registry uses BACKWARD compatibility by default. Kafka ingestion can parse the Confluent wire header, retain schema IDs/raw bytes and optionally resolve the schema document through `schemaRegistryUrl`. Register schemas and enforce compatibility in producer/consumer release pipelines. This example uses PLAINTEXT on a private Compose network; use TLS/SASL, ACLs and separate failure domains before cross-host production exposure.

## Load, stress and soak tests

The `perf/k6` directory contains smoke, stress and 24-hour soak scenarios; see [perf/README.md](../perf/README.md) for commands and acceptance gates. The included k6 workload is an API baseline, not a replacement for dedicated Kafka ingestion, Spark pipeline, Doris analytical-query and S3 throughput benchmarks. Run all profiles in staging, capture Prometheus metrics and test broker/FE/BE loss, checkpoint recovery, DLQ replay and database restore during sustained traffic. Never run destructive tests against customer production data.
