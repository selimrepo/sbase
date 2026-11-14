# Production deployment and recovery

## Deployment profile

The default Compose stack is a local evaluation and integration environment.
It runs one PostgreSQL database and Spark master/worker; it is not a production
high-availability topology.

Provision durable encrypted storage, a private network, a TLS reverse proxy and
a secret manager before production rollout. Set these values in the protected
deployment environment (never commit them):

- `POSTGRES_PASSWORD`: high-entropy PostgreSQL application/database password.
- `JWT_ISSUER_URI` and `JWT_JWK_SET_URI`: production identity issuer and JWK endpoint.
- `SBASE_IMAGE`: immutable, pinned application image.
- `SBASE_BIND_ADDRESS`: defaults to loopback so a host reverse proxy can terminate TLS.
- `PROMETHEUS_SCRAPE_TOKEN_FILE`: path to the monitoring token secret.

Start with:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml config
docker compose -f docker-compose.yml -f docker-compose.production.yml up -d
```

PostgreSQL and Spark ports are not published to the host by the production
overlay. The app is bound to loopback by default. Only trusted reverse proxies
should supply forwarded headers. Prometheus is internal to the Compose network
and its scrape token is mounted as a Docker secret.

## PostgreSQL backup and restore

Create a logical backup:

```bash
export POSTGRES_PASSWORD='...'
COMPOSE_FILE=docker-compose.yml:docker-compose.production.yml \
  SBASE_BACKUP_DIR=/secure/backup/path bash ops/backup-postgres.sh
```

Restore is destructive and requires explicit confirmation:

```bash
export POSTGRES_PASSWORD='...'
COMPOSE_FILE=docker-compose.yml:docker-compose.production.yml \
  CONFIRM_DESTRUCTIVE_RESTORE=YES bash ops/restore-postgres.sh /secure/backup/path/sbase.dump
```

Copy backups to encrypted off-host storage and verify checksums. A logical
backup is not point-in-time recovery. Production PostgreSQL should also have WAL
archiving and tested PITR (for example pgBackRest or an equivalent managed
service). Restore drills must verify Flyway state, tenant governance registry,
catalog metadata, pipelines, schedules, BI reports and online feature values.
Object-store artifacts require their own versioning and replication policy.

## Availability and resource planning

The `app-ha` Compose profile adds a second SBase application process only; it
does not make PostgreSQL highly available. For production, use a managed
PostgreSQL HA service or a tested primary/standby design with automatic
failover, connection routing, WAL archiving and durable storage.

Size PostgreSQL connection limits across all application replicas, Spark
executors and operational clients. Monitor replication lag, connection
saturation, deadlocks, long transactions, vacuum age, table/index bloat and
disk headroom. Test application reconnect behavior after PostgreSQL failover.

## Security and tenant boundaries

Use a least-privilege runtime role and keep migration/owner credentials separate.
Enable TLS for cross-host database traffic. Tenant-owned analytical tables must
be explicitly allow-listed, have a verified `tenant_id` column, and have a
physical UNIQUE constraint/index whose idempotency key includes `tenant_id`.
Spark reads push tenant filters into PostgreSQL; Spark writes use run-scoped
UNLOGGED staging tables and `INSERT .. ON CONFLICT DO NOTHING`.

Application predicates and tenant governance metadata are not substitutes for
database grants or PostgreSQL row-level security. Enable RLS where appropriate,
ensure the runtime role cannot bypass it, and add negative integration tests for
every new tenant-facing repository, ingestion connector, scheduled worker and
artifact path before enabling shared multi-customer SaaS.
