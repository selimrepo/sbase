# SBase

**SBase** is an open-source, KNIME-inspired data science, analytics and data-mining server. The target is a visual, reproducible workspace for data ingestion, preparation, distributed processing, mining, model lifecycle and operational analytics.

> Project status: foundation scaffold. This repository is being rebuilt from an empty baseline. The architecture and roadmap describe intended capabilities; they are not claims that every engine is already integrated or production-certified.

## Architecture

- **Scala + Apache Pekko** — service runtime, typed actors, clustering and distributed job coordination.
- **PostgreSQL** — transactional control plane: projects, workflows, versions, execution metadata and job state.
- **Apache Spark** — batch analytics and large-scale feature engineering.
- **Apache Mahout** — distributed and mathematically oriented mining algorithms.
- **Apache Flink** — streaming and event-time processing.
- **Apache Cassandra** — wide-column storage for high-volume operational and time-series data.
- **Apache Solr** — full-text, faceted and metadata search.
- **Apache Geode** — distributed in-memory data grid and low-latency shared state.
- **React (planned)** — KNIME-like visual workflow and data science studio.

Authentication is intentionally out of scope for this deployment. Do not add Keycloak, OAuth/OIDC, MFA, registration or login flows. Network isolation and infrastructure-level access controls remain the operator's responsibility.

## Repository layout

```
.
├── project/                 # sbt build configuration
├── src/main/scala/          # Pekko service
├── src/main/resources/db/   # PostgreSQL migrations
├── src/test/scala/          # automated tests
├── docker/                  # engine-specific configuration
├── compose.yaml             # local development services
├── Dockerfile
└── .github/workflows/ci.yml
```

## Requirements

- JDK 17+
- sbt 1.10+
- Docker Engine + Docker Compose v2

## Run locally

1. Copy `.env.example` to `.env` and adjust ports/passwords.
2. Start infrastructure: `docker compose up -d postgres cassandra solr`
3. Run the service: `sbt run`
4. Check `http://localhost:8080/health` and `http://localhost:8080/api/v1`.

Optional engines are behind Compose profiles:

- `docker compose --profile spark up -d`
- `docker compose --profile flink up -d`
- `docker compose --profile geode up -d`

The optional profiles are development topology starters. They are not a production HA deployment.

## Configuration

All settings are environment driven. See `.env.example`. Never commit real credentials. PostgreSQL is the authoritative OLTP/control-plane store; Cassandra, Solr and Geode are purpose-specific stores and must not become competing sources of truth.

## Engineering principles

1. PostgreSQL transactions own workflow definitions, versions, leases and durable execution state.
2. Every workflow run has an immutable workflow-version reference and correlation ID.
3. Engine adapters are isolated behind interfaces; a workflow definition must not depend on a particular execution engine.
4. Long-running work is asynchronous, idempotent and recoverable after worker restart.
5. Backpressure, bounded concurrency, deadlines, cancellation and retry policies are explicit.
6. Secrets are injected at runtime; logs never include credentials or raw secret-bearing configuration.
7. Integration tests use real service containers where behavior depends on the external engine.

## Roadmap

- [x] Establish Scala/Pekko repository foundation and local infrastructure topology.
- [ ] Durable PostgreSQL workflow/job model, migrations and transactional outbox.
- [ ] Pekko cluster, sharding, worker leases, retries and recovery.
- [ ] Typed connector contracts for PostgreSQL, Cassandra, Solr and Geode.
- [ ] Spark batch execution and Mahout mining operators.
- [ ] Flink streaming jobs, checkpoints and savepoint lifecycle.
- [ ] Visual workflow editor, typed node catalog, validation and execution preview.
- [ ] Dataset catalog, lineage, profiling, quality rules and feature engineering.
- [ ] Experiment tracking, model registry, evaluation and model serving.
- [ ] Metrics, tracing, structured logs, HA/DR and security hardening.
- [ ] Multi-hour soak, fault-injection, load and recovery tests.

## License

Apache License 2.0 (see `LICENSE`).
