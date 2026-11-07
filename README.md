# SBase — Data Science & Data Mining Hub

Spring Boot + PostgreSQL/JPA metadata + Spark + Mahout + Apache Doris.

## Run
```bash
cp .env.example .env
docker compose up --build -d
```

## Core APIs
- Mining jobs: `/api/mining/jobs`
- Dataset catalog: `/api/datasets`
- Pipeline definitions: `/api/pipelines`
- Mahout algorithms: `GET /api/algorithms/mahout`, `POST /api/algorithms/mahout/score` (cosine and Euclidean adapters)
- Platform metadata: `/api/platform/experiments`, `/api/platform/models`, `/api/platform/models/{id}/versions`, `/api/platform/features`, `/api/platform/schedules`, `/api/platform/dashboards?owner=...`
- BI inventory: `GET /api/platform/bi/summary`

## Capability foundations
Migration V3 adds ingestion connector/run metadata, experiment tracking, registered models and immutable numbered model versions, feature-set definitions, persistent schedule definitions, role/permission/subject mapping tables, dashboard and widget metadata. `IngestionConnector` is the extension SPI for Doris/JDBC/object-store/stream connectors. Mahout adapters use Mahout vectors for cosine similarity and expose Euclidean distance alongside it.

## Execution and security services

- Ingestion execution: `POST /api/platform/ingestion/{jdbc|doris|kafka}/runs`; connector types are discovered at `GET /api/platform/ingestion/connectors`. JDBC/Doris use bounded batch inserts; Kafka consumption is bounded by `maxRecords` and `pollTimeoutMs`.
- Spark feature materialization: `POST /api/platform/features/materialize` with Parquet input/output URIs and allow-listed operations (`copy`, `cast_double`, `log1p`, `normalize_zscore`, `bucket`).
- S3-compatible artifact storage: `S3ArtifactStore` supports S3/MinIO endpoint overrides, bucket, region and credentials through environment variables.
- Distributed schedule polling: PostgreSQL `FOR UPDATE SKIP LOCKED` claims due jobs with expiring leases, retry counters and exponential backoff.
- JWT resource-server validation: configure `JWT_ISSUER_URI`; role and permission claims are mapped to Spring Security authorities. Database role-permission checks are available through `PermissionService`.
- Doris BI datasource: a dedicated JDBC datasource and allow-listed table query service are configured with `DORIS_ALLOWED_TABLES`.

## Production limitations
These are persisted capability foundations and metadata APIs, not a finished managed platform. The connector SPI still needs concrete source adapters and run orchestration. Schedules are stored but require a distributed lease/dispatch worker before execution. Feature definitions are not yet materialized or served online. Model artifact URIs require an external object store and lifecycle policy. Role tables are not yet wired into JWT/OIDC authorization; secure the API behind an authenticated gateway and enforce dataset-level permissions before multi-user production. Dashboard metadata is stored; arbitrary SQL execution is intentionally not exposed. BI summary currently reports PostgreSQL metadata counts, not Doris query analytics.

Run `gradle clean test bootJar` before deployment. CI must pass before treating this branch as production-ready.
