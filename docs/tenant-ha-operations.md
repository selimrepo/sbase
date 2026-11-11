# Tenant, HA and operational controls

## Tenant identity and ownership

The API now requires a UUID `tenant_id` (or `tenant`) claim in the verified JWT. A token without it is denied. Tenant IDs are not accepted from request bodies or query parameters. Migration V13 backfills existing datasets, pipelines and pipeline runs into the reserved `legacy-default` tenant; existing records are not silently exposed to newly provisioned tenants.

Datasets and pipelines carry `tenant_id` and `owner_subject`. Their list/get/update/delete paths are tenant-scoped. Resource grants are tenant-scoped and support READ, WRITE and ADMIN. Grant management is exposed at `/api/access-grants` and requires `resources:admin`.

Example grant:

```http
POST /api/access-grants
Authorization: Bearer <tenant-scoped-token>
Content-Type: application/json

{"resourceType":"DATASET","resourceId":"<uuid>","subject":"analyst@example.com","permission":"READ"}
```

Pipeline run read/submit endpoints first authorize access to the owning pipeline. The current rollout does not yet cover every platform object (models, artifacts, schedules, feature sets, mining jobs and all connector registrations) with tenant filters. Do not enable shared multi-tenant production until those paths and background workers are covered and cross-tenant integration tests pass. Existing global workers need explicit tenant context before operating on tenant data.

## Audit and API throttling

Every `/api/**` request is recorded in `audit_events` with subject, tenant claim, route, method, outcome, status, request ID, peer IP and user agent. Avoid putting secrets or request bodies in audit records. The database-backed token bucket applies after JWT authentication and keys by subject and normalized route; unauthenticated requests are keyed by socket peer IP. Do not trust forwarded IP headers unless the reverse proxy trust chain is explicitly configured. For high-volume deployments, replace the single PostgreSQL bucket table with a distributed Redis limiter and partition/retention policy for audit events.

## Doris high availability

Use the additional topology overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml -f docker-compose.doris-ha.yml config
docker compose -f docker-compose.yml -f docker-compose.production.yml -f docker-compose.doris-ha.yml up -d
```

It defines three FE nodes and three BE nodes with persistent volumes. Place them on separate hosts/AZs for actual node/host failure tolerance. Verify FE membership (`SHOW FRONTENDS`), backend liveness (`SHOW BACKENDS`), tablet replica health and replication factor after bootstrap. Configure `default_replication_num=3` for newly created tables and migrate existing tables deliberately. The base one-FE/one-BE profile remains for CI and local development. This Compose overlay is a topology template and must be validated against the exact Doris image startup scripts before production rollout.

## Doris snapshot / restore

Scripts `ops/backup-doris.sh` and `ops/restore-doris.sh` use Doris S3 repositories. Provide a dedicated least-privilege S3 identity and an S3-compatible endpoint. The scripts accept an explicit comma-separated table list and use a stable snapshot label. A successful SQL submission is not proof of a completed backup: check `SHOW BACKUP FROM analytics`, copy the repository data off failure domain, and perform restore drills. Restore is destructive and requires `CONFIRM_DORIS_RESTORE=YES`.

## S3 / MinIO object lifecycle

Start the optional MinIO overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml -f docker-compose.object-storage.yml up -d
```

It creates the `sbase-artifacts` bucket, enables versioning, expires current objects after 365 days and non-current versions after 90 days. Set `SBASE_S3_ENDPOINT=http://minio:9000`, `SBASE_S3_ACCESS_KEY`, `SBASE_S3_SECRET_KEY` and `SBASE_S3_BUCKET=sbase-artifacts` for the application. In production use a distributed MinIO cluster or external S3 with replication, encryption, object lock where required, and off-site backup. Lifecycle expiry must be aligned with legal retention and model/artifact references.

## Kafka HA and schema evolution

Start the Kafka KRaft overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka-ha.yml up -d
```

Three broker/controller nodes use replication factor 3 and min ISR 2 for internal topics. Schema Registry uses BACKWARD compatibility by default. Register schemas and enforce compatibility in producer/consumer release pipelines; do not switch compatibility modes casually. This example uses PLAINTEXT on the private Compose network and is not a secure cross-host deployment. Use TLS/SASL, ACLs, separate controller/broker roles and separate failure domains for production. Set connector bootstrap servers to all broker endpoints.

## Load, stress and soak tests

The `perf/k6` directory contains smoke, stress and long-duration soak scenarios. Run with a production-like staging environment and a scoped test token. Track p95/p99 latency, error rate, throughput, DB pool saturation, Kafka lag, Doris query/compaction pressure, memory, CPU and disk. Soak tests should run for at least 24 hours before a release candidate; test broker/BE/FE loss and recovery during sustained traffic. Never run destructive stress tests against customer production data.
