# SBase production-readiness test suite

Deployment-facing checks for a staging environment. Do not run load or restore tests against production without an approved maintenance window.

## Prerequisites

Bash, curl, PostgreSQL client tools (psql/pg_dump/pg_restore), k6, and a deployed SBase environment.

## Environment configuration

Docker Compose automatically reads a project-root `.env` file. Create it from the template and replace the placeholder with a strong, unique password:

```bash
cp .env.example .env
# Generate a secret, then paste it as POSTGRES_PASSWORD in .env:
openssl rand -base64 36
chmod 600 .env
docker compose config
docker compose up -d --build
```

`POSTGRES_PASSWORD` is mandatory: Compose refuses to start if it is missing or empty. The same value is injected into PostgreSQL and the SBase application as `SPRING_DATASOURCE_PASSWORD`. The checked-in `.env.example` is only a template; never commit the real `.env` file. For running Spring Boot outside Compose, export `SPRING_DATASOURCE_PASSWORD` in the process environment.

## Development and production Compose files

The shared `docker-compose.yml` retains the complete service graph and optional profiles (HA, PostgreSQL HA, Kafka, object storage and observability). Use one environment overlay; do not start both overlays together.

Development:

```bash
docker compose -f docker-compose.yml -f docker-compose.development.yml config
docker compose -f docker-compose.yml -f docker-compose.development.yml up --build -d
```

Production (pin `SBASE_IMAGE` to an immutable release tag or digest and provide production-only values in the deployment environment or secret manager):

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml config
docker compose -f docker-compose.yml -f docker-compose.production.yml up -d
```

To enable an optional profile, append the same `--profile` flag to either command, for example `--profile object-storage` or `--profile observability`. The base Compose file owns shared infrastructure; the overlay changes only environment-specific settings. Spring Boot settings remain in `application.yml` and the profile-specific `application-development.yml` / `application-production.yml` files. The Compose overlay activates the matching Spring profile.

## Smoke and readiness

\`\`\`bash
export SBASE_BASE_URL=https://sbase-staging.example.com
bash scripts/production-readiness/smoke.sh
\`\`\`

Optional read-only business endpoint: set SBASE_CHECK_API_PATH and optionally SBASE_CHECK_API_EXPECTED_STATUS.

## Security

\`\`\`bash
export SBASE_BASE_URL=https://sbase-staging.example.com
export POSTGRES_PASSWORD='injected-secret'
export SPRING_DATASOURCE_PASSWORD='injected-secret'
export JWT_ISSUER_URI='https://identity.example.com/realms/sbase'
export JWT_JWK_SET_URI='https://identity.example.com/realms/sbase/protocol/openid-connect/certs'
export SBASE_PROTECTED_PATHS=$'/api/datasets\n/api/pipelines'
bash scripts/production-readiness/security.sh
\`\`\`

The script checks credential injection, HTTPS, anonymous access to sensitive Actuator endpoints, selected protected routes and basic browser security headers. Supply representative protected routes from the deployed API. The route list is configurable because API paths can change.

Additional mandatory security gates:
- OWASP ZAP baseline/full scan against staging; triage all high/critical findings.
- Two-principal tenant isolation tests for reads, writes, deletes, exports, BI reports, ML artifacts and job status.
- Reject expired, malformed, wrong-issuer, wrong-audience and wrong-signature JWTs.
- Verify role/permission matrix for every state-changing endpoint.
- Scan source, dependencies, container images and IaC; generate SBOM and sign release images.
- Verify secrets never appear in logs, Actuator, exception bodies or artifacts.

## PostgreSQL backup and restore

The script creates and drops only a uniquely named scratch database prefixed sbase_pr_restore_. The database user needs dump permissions and CREATE/DROP DATABASE privileges on the PostgreSQL test instance.

\`\`\`bash
export PGHOST=staging-db.example.com PGPORT=5432 PGUSER=sbase_backup
export PGPASSWORD='test-secret' PGDATABASE=sbase
bash scripts/production-readiness/backup-restore.sh
\`\`\`

This validates logical dump/restore and base-table count, not point-in-time recovery, encryption, off-site retention, RPO or RTO. Run those drills separately.

## Performance

\`\`\`bash
export SBASE_BASE_URL=https://sbase-staging.example.com
export SBASE_PERF_PATH=/actuator/health/readiness
k6 run scripts/production-readiness/k6-load.js
\`\`\`

For a meaningful application benchmark, choose a representative read-only endpoint and provide a short-lived staging token:

\`\`\`bash
export SBASE_PERF_PATH=/api/your-read-only-route
export SBASE_BEARER_TOKEN='short-lived-token'
K6_VUS=50 K6_HOLD=10m k6 run scripts/production-readiness/k6-load.js
\`\`\`

Initial thresholds: failed requests below 1%, p95 below 500 ms, p99 below 1.5 s. Agree actual SLOs and realistic data volume before release. Load against the health endpoint is only an infrastructure smoke benchmark.

## Data integrity and resilience scenarios

Existing CI already exercises Spark-to-PostgreSQL load, duplicate-key idempotency, PostgreSQL outage/recovery, app liveness/restart. Add staging drills for:
- Concurrent duplicate submissions with one idempotency key; one durable logical result.
- Worker termination between external write and status update; no duplicate business rows on retry.
- Lease expiry, retry exhaustion, dead-letter visibility and manual replay.
- Schema drift, nullability/type violations and rejected destructive writes.
- Concurrent BI runs, cancellation, timeout, row/cost limits and tenant-scoped sharing.
- ML feature replay, artifact checksum verification and tenant isolation.
- Database restart during writes; rollback and Flyway upgrade from the previous supported release.
- Restore a backup, verify selected business aggregates/checksums and measure RPO/RTO.

## Release gate

Do not call a release production-ready until applicable checks have evidence, critical/high security findings are resolved or formally accepted, restore and rollback drills pass, and load tests meet agreed SLOs at expected peak volume. Store reports with release tag, commit SHA, environment version and timestamp.


## Static production configuration audit

Run this before deploying. It intentionally detects the insecure development password fallbacks currently present in the checked-in Compose/Spring configuration; it should remain red until those defaults are removed and explicit secrets are required.

\`\`\`bash
bash scripts/production-readiness/config-audit.sh
\`\`\`
