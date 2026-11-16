# SBase

SBase is a KNIME-inspired distributed data science and mining server. The current `develop` branch contains the first runnable workflow execution foundation. It is not yet a fully production-certified enterprise platform.

## Stack

- Scala 2.13 + Gradle
- Apache Pekko Typed, Cluster and Cluster Sharding
- PostgreSQL + Flyway for OLTP, immutable workflow versions, durable jobs, leases and execution events
- Apache Spark SQL for batch operators
- Apache Mahout math for mining operators
- Apache Flink REST for submitting pre-uploaded JARs and inspecting running jobs
- React + React Flow for visual workflow authoring
- Cassandra, Solr and Geode remain planned connector integrations in this development phase

Authentication is intentionally out of scope. No login, registration, OAuth/OIDC, MFA or Keycloak components are included. Protect deployments with private networks, firewalls and TLS at the infrastructure edge.

## Run the stack

Requirements: JDK 17+, Gradle 8.14.3+, Docker Engine and Compose v2.

```bash
cp .env.example .env
docker compose up --build
```

- Studio: http://localhost:3000
- API: http://localhost:8080/api/v1
- Health: http://localhost:8080/health
- PostgreSQL: localhost:5432

Run backend tests without Docker except for PostgreSQL Testcontainers tests (Docker required):

```bash
gradle --no-daemon clean test installDist

# When running the server directly on the host (instead of Compose):
SBASE_CLUSTER_HOST=127.0.0.1 SBASE_CLUSTER_SEEDS=pekko://sbase@127.0.0.1:25520 gradle run
```

Build the studio:

```bash
cd web
npm install
npm run build
```

## Workflow API

Create a workflow with an immutable version 1 definition:

```http
POST /api/v1/workflows
Content-Type: application/json

{
  "name": "Vector mining",
  "definition": {
    "nodes": [
      {
        "id": "sum",
        "operator": "mahout-vector-sum",
        "config": { "values": [1, 2, 3] }
      }
    ],
    "edges": []
  }
}
```

Submit a run (the idempotency key must be unique per logical submission):

```http
POST /api/v1/workflows/{workflowId}/runs
Content-Type: application/json

{ "idempotencyKey": "run-001", "maxAttempts": 3 }
```

Poll the job:

```http
GET /api/v1/jobs/{jobId}
```

Available operators:

- `mahout-vector-sum`: runs a Mahout DenseVector reduction.
- `spark-sql`: executes a Spark SQL statement. Set `SPARK_MASTER` to the cluster master URL for distributed execution; defaults to local mode for development.
- `flink-submit`: submits a previously uploaded Flink JAR by `jarId`; optional `entryClass`, `parallelism` and `programArgs` can be supplied.
- `flink-jobs-overview`: reads Flink's jobs overview endpoint.

For Flink, set `FLINK_REST_URL` (Compose profile default is `http://flink-jobmanager:8081`). The Flink JAR upload step is an external artifact publication operation; this adapter invokes the Flink REST run endpoint.

## Durable execution design

1. Workflow definitions are JSONB snapshots with SHA-256 checksums and immutable version references.
2. Job submission is idempotent and stored transactionally in PostgreSQL.
3. Workers claim jobs using `FOR UPDATE SKIP LOCKED`.
4. Each claim receives a fresh database-generated fencing token and lease expiry.
5. Sharded Pekko worker actors execute the workflow and periodically heartbeat the lease.
6. A worker that loses its lease cannot commit a stale result.
7. Expired leases are recovered; exponential retry backoff is applied to failed attempts.
8. Jobs that exhaust their retry budget are terminally failed instead of being requeued forever.
9. Execution events are recorded for queue, lease, success and failure transitions.

Pekko seed nodes and advertised/bind addresses can be configured with `SBASE_CLUSTER_SEEDS`, `SBASE_CLUSTER_HOST`, `SBASE_CLUSTER_PORT`, `SBASE_CLUSTER_BIND_HOST` and `SBASE_CLUSTER_BIND_PORT`. Use stable, directly reachable node addresses for multi-node deployments.

## CI

GitHub Actions runs:

- Gradle Scala compilation, JUnit unit tests, PostgreSQL Testcontainers integration tests and application distribution packaging.
- React production build.
- Backend Docker image build.

The Compose file includes optional development profiles for Cassandra, Solr, Spark and Flink. They are not an HA/DR production topology.

## Roadmap

- [x] Gradle Scala + Pekko service foundation
- [x] Pekko Cluster Sharding worker region
- [x] PostgreSQL durable queue, lease fencing, heartbeats, retries and expired-lease recovery
- [x] Mahout vector mining and Spark SQL operators
- [x] Flink REST job submission and job overview
- [x] React Flow workflow graph editor and job polling
- [ ] Node input/output contracts and persisted per-node execution previews
- [ ] Spark structured streaming and Flink checkpoint/savepoint lifecycle management
- [ ] Cassandra, Solr and Geode production connector implementations
- [ ] Outbox dispatcher, scheduler, resource quotas and cancellation
- [ ] OpenTelemetry metrics/traces, alerting, HA/DR and restore drills
- [ ] Multi-node chaos, load and week-long soak verification

## License

Apache License 2.0. See `LICENSE`.
