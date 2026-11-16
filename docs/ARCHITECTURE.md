# SBase architecture

## Control plane and execution plane

The control plane is a Scala service backed by PostgreSQL. It owns workspace and workflow metadata, immutable workflow versions, execution state, idempotency keys and the transactional outbox. PostgreSQL is the source of truth for orchestration state.

The execution plane is composed of Pekko-managed coordinators and workers. Engine adapters submit bounded units of work to Spark, Flink or local operators. Cassandra, Solr and Geode are accessed through purpose-specific connector interfaces.

## Intended workflow lifecycle

1. Author a typed node graph in the studio.
2. Validate node schemas, ports, configuration and graph acyclicity.
3. Persist an immutable workflow version and checksum.
4. Submit a run with a client idempotency key.
5. Commit the run record and outbox event in one PostgreSQL transaction.
6. Dispatch through Pekko; acquire a lease before execution.
7. Execute operators with bounded concurrency and explicit resource requirements.
8. Persist node/run state, metrics and lineage.
9. Recover expired leases and retry only retryable, idempotent operations.
10. Publish outputs and terminal state.

## Engine boundaries

- Spark: batch DataFrame and ML pipeline execution.
- Mahout: selected mining operators, not a replacement for the workflow runtime.
- Flink: continuous stream jobs with checkpoint/savepoint semantics.
- Cassandra: high-volume wide-column data, not workflow metadata.
- Solr: derived search index, rebuildable from authoritative records.
- Geode: distributed cache/data grid; never the only copy of durable state.

## Production gates

Before production use, implement and verify cluster membership, split-brain strategy, durable leases, fencing tokens, outbox delivery, dead-letter handling, resource quotas, secret management, TLS, network policy, backup/restore drills, metrics/traces, load tests and failure-injection tests. The initial Compose setup is for development only.
