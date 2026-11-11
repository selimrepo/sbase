# Production deployment and recovery

## Production profile

The default `docker-compose.yml` is a local evaluation/integration environment. It intentionally remains convenient for CI and local development and is **not** a hardened production topology.

Use the production overlay only after provisioning a private Docker network, reverse proxy with TLS, durable encrypted storage, and secrets:

```bash
mkdir -p secrets backups/postgres
chmod 700 secrets backups backups/postgres
printf '%s' 'A-long-random-monitoring-token' > secrets/prometheus_scrape_token.txt
chmod 600 secrets/prometheus_scrape_token.txt
```

Set the following values in the deployment environment or a protected `.env` file (never commit it):

- `POSTGRES_PASSWORD`: unique high-entropy password.
- `DORIS_USERNAME` and `DORIS_PASSWORD`: dedicated Doris account; do not use root for the application.
- `JWT_ISSUER_URI` and `JWT_JWK_SET_URI`: issuer and JWK endpoint of the production identity provider.
- `SBASE_BIND_ADDRESS`: defaults to 127.0.0.1 so a host reverse proxy can terminate TLS.
- `SBASE_DB_POOL_MAX`: size the pool against PostgreSQL max_connections and the number of app replicas.

Start with:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml config
docker compose -f docker-compose.yml -f docker-compose.production.yml up -d --build
```

The application is bound to loopback by default. Put a TLS reverse proxy in front of it and configure forwarded headers only when the proxy is trusted. PostgreSQL, Doris FE/BE and Spark ports are not published to the host by the production overlay. The monitoring token file is a Docker secret; grant `ROLE_MONITORING` only to the monitoring identity used by Prometheus. Prometheus is internal to the Compose network and has no published host port.

The overlay applies conservative resource limits for a single-node deployment. These are starting limits, not capacity recommendations. A production Doris deployment still requires a multi-FE/multi-BE topology, replication appropriate to the failure domain, dedicated storage, and tested recovery procedures. The bundled single FE/BE topology is not highly available.

## PostgreSQL backup and restore

Create a custom-format backup:

```bash
export POSTGRES_PASSWORD='...'
SBASE_BACKUP_DIR=/secure/backup/path ./ops/backup-postgres.sh
```

The script writes a checksum and retains local backups for 14 days by default. Copy both the dump and checksum to encrypted off-host storage; local Docker volumes are not a backup.

Restore is destructive and requires an explicit confirmation:

```bash
export POSTGRES_PASSWORD='...'
CONFIRM_DESTRUCTIVE_RESTORE=YES ./ops/restore-postgres.sh /secure/backup/path/sbase-YYYYMMDDTHHMMSSZ.dump
```

Perform restore drills on an isolated environment and verify Flyway state, dataset metadata, pipeline runs, schedules and online feature values. PostgreSQL backup does not back up Doris data or S3-compatible artifacts. Configure Doris snapshot/restore and object-store versioning/replication independently before production use.

## Security boundary

The application uses stateless bearer-token authentication for API routes. Health probes and informational metadata are public; metrics endpoints require `ROLE_MONITORING` or `ROLE_ADMIN`. The React static workspace remains public, while its API calls require a valid token. The security headers include a restrictive Content Security Policy; validate any external identity-provider or asset integration against this policy before rollout.

This production profile does **not** by itself implement tenant isolation or row-level dataset authorization. Do not expose a shared multi-customer deployment until tenant-aware ownership has been applied consistently to datasets, pipelines, runs, schedules, feature sets, models, artifacts, ingestion registrations and every read/write path. It also does not make the single-node Doris topology highly available. These require dedicated schema/domain changes and cross-cutting integration tests, not a deployment toggle.
