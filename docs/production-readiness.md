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
- `(removed)` and `(removed)`: dedicated PostgreSQL account; bootstrap creates this account and grants access only to `analytics.*`. Use a URL-safe password containing no single quote or backslash. The PostgreSQL root account remains for internal initialization only.
- `JWT_ISSUER_URI` and `JWT_JWK_SET_URI`: issuer and JWK endpoint of the production identity provider.
- `SBASE_BIND_ADDRESS`: defaults to 127.0.0.1 so a host reverse proxy can terminate TLS.
- `SBASE_DB_POOL_MAX`: size the pool against PostgreSQL max_connections and the number of app replicas.

Start with:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml config
docker compose -f docker-compose.yml -f docker-compose.production.yml up -d --build
```

The application is bound to loopback by default. Put a TLS reverse proxy in front of it and configure forwarded headers only when the proxy is trusted. PostgreSQL, PostgreSQL FE/BE and Spark ports are not published to the host by the production overlay. The monitoring token file is a Docker secret; grant `ROLE_MONITORING` only to the monitoring identity used by Prometheus. Prometheus is internal to the Compose network and has no published host port.

The overlay applies conservative resource limits for a single-node deployment. These are starting limits, not capacity recommendations. A production PostgreSQL deployment still requires a multi-FE/multi-BE topology, replication appropriate to the failure domain, dedicated storage, and tested recovery procedures. The bundled single FE/BE topology is not highly available.

## PostgreSQL backup and restore

Create a custom-format backup:

```bash
export POSTGRES_PASSWORD='...'
COMPOSE_FILE=docker-compose.yml:docker-compose.production.yml SBASE_BACKUP_DIR=/secure/backup/path bash ops/backup-postgres.sh
```

The script writes a checksum and retains local backups for 14 days by default. Copy both the dump and checksum to encrypted off-host storage; local Docker volumes are not a backup.

Restore is destructive and requires an explicit confirmation:

```bash
export POSTGRES_PASSWORD='...'
COMPOSE_FILE=docker-compose.yml:docker-compose.production.yml CONFIRM_DESTRUCTIVE_RESTORE=YES bash ops/restore-postgres.sh /secure/backup/path/sbase-YYYYMMDDTHHMMSSZ.dump
```

Perform restore drills on an isolated environment and verify Flyway state, dataset metadata, pipeline runs, schedules and online feature values. PostgreSQL backup does not back up PostgreSQL data or S3-compatible artifacts. Configure PostgreSQL snapshot/restore and object-store versioning/replication independently before production use.

## Security boundary

The application uses stateless bearer-token authentication for API routes. Health probes and informational metadata are public; metrics endpoints require `ROLE_MONITORING` or `ROLE_ADMIN`. The React static workspace remains public, while its API calls require a valid token. The security headers include a restrictive Content Security Policy; validate any external identity-provider or asset integration against this policy before rollout.

Tenant membership, ownership metadata and resource ACL foundations exist. Pipeline PostgreSQL reads/writes, ML experiment/run/model/deployment access, tenant-filtered ML training reads, model artifact path validation, governed BI queries/cache, BI reports/dashboards/widgets and legacy dashboard layout mutations now enforce tenant boundaries. PostgreSQL integration tests exercise cross-tenant feature values, ML records/training source selection, BI resources and artifact path escape rejection. This is still not complete row-level isolation across every legacy query, ingestion connector, artifact lifecycle, schedule and distributed worker path. Shared multi-customer SaaS remains unsupported until negative cross-tenant integration tests cover every data-plane and background path. The single-node PostgreSQL topology is also not highly available; use the dedicated HA profile and validate real multi-host failure domains.
