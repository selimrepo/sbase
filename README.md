# SBase

**SBase** is an open-source, KNIME-inspired data science and data-mining server. It is designed around a visual, reproducible workflow environment for data ingestion, preparation, distributed analytics, mining, model lifecycle and operational reporting.

> Current status: foundational scaffold on the `develop` branch. The roadmap describes intended capabilities; components are not production-ready until their integrations, recovery behavior and tests are implemented.

## Technology architecture

| Area | Technology | Responsibility |
|---|---|---|
| Platform runtime | Scala 2.13 + Apache Pekko | Typed actors, service runtime, clustering and distributed coordination |
| Build | Gradle + Scala plugin | Reproducible build, tests and application packaging |
| OLTP/control plane | PostgreSQL | Workspaces, workflow versions, jobs, leases, lineage metadata |
| Batch analytics | Apache Spark | Distributed SQL, batch transformations and feature engineering |
| Mining | Apache Mahout | Mathematical and distributed mining algorithms |
| Streaming | Apache Flink | Stateful stream processing, event time and checkpoints |
| NoSQL | Apache Cassandra | High-volume wide-column and time-series workloads |
| Search | Apache Solr | Full-text, faceted and metadata search |
| In-memory grid | Apache Geode | Distributed cache and low-latency shared data |
| Visual studio | React (planned) | Node-based workflow editor, previews and execution monitoring |

Authentication is deliberately out of scope. Do not add Keycloak, OAuth/OIDC, MFA, registration or login flows. Operators must protect deployments through network isolation, TLS termination and infrastructure-level access controls.

## Run locally

Requirements: JDK 17+, Gradle 8.14.3+, Docker Engine and Compose v2.

```bash
cp .env.example .env
docker compose up -d postgres
gradle clean test run
```

Health endpoint: `http://localhost:8080/health`

Optional development engines: `docker compose --profile nosql --profile search up -d`, `docker compose --profile spark up -d`, `docker compose --profile flink up -d`.

The Compose topology is for local development, not production HA.

## Engineering principles

1. PostgreSQL is the source of truth for workflow definitions, versions and durable execution state.
2. Workflow versions are immutable and checksum-addressable.
3. Engine adapters isolate workflow semantics from Spark, Flink and other engines.
4. Long-running work is asynchronous, idempotent and recoverable after worker restart.
5. Backpressure, bounded concurrency, deadlines, cancellation and retries are explicit.
6. Search indexes and caches are derived data, not authoritative state.
7. Credentials are injected at runtime and never committed.
8. Integration tests use real service containers when correctness depends on external behavior.

## Roadmap

- [x] Gradle + Scala project foundation and Pekko HTTP service.
- [x] Initial PostgreSQL control-plane schema.
- [ ] Pekko Cluster/Sharding, durable leases, fencing and worker recovery.
- [ ] Transactional outbox and idempotent job dispatch.
- [ ] Typed connectors for PostgreSQL, Cassandra, Solr and Geode.
- [ ] Spark batch execution and Mahout mining operators.
- [ ] Flink streaming lifecycle, checkpoints and savepoints.
- [ ] React workflow studio with typed nodes, validation, previews and live run status.
- [ ] Dataset catalog, lineage, profiling, data quality and feature engineering.
- [ ] Experiment tracking, model registry, evaluation and model serving.
- [ ] Metrics, tracing, structured logs, backup/restore and HA/DR.
- [ ] Load, soak, chaos and disaster-recovery verification.

## License

Apache License 2.0. See `LICENSE`.
