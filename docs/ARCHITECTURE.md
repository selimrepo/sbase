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


## Scientific AutoML and model governance

The AutoML lifecycle uses Spark ML Pipelines so fitted preprocessing is persisted with the estimator. Numeric imputation is trained within each training fold; categorical indexers and encoders are also fold-local. Candidate selection uses configurable metrics and bounded parameter grids, then reports independent holdout metrics against a simple baseline.

Each completed experiment stores a model card in PostgreSQL with a SHA-256 training-data fingerprint, schema, seed, Spark version, split protocol, feature missingness/profile, validation metrics and optional binary fairness diagnostics. The artifact and its model card are separate: the model artifact is stored under the configured registry path while governance metadata remains queryable in PostgreSQL.

Time-series runs generate lagged targets with a Spark window ordered by the declared time column. They require unique timestamps and use a chronological holdout; random-fold cross-validation is disabled for this task because it would leak temporal information. Rolling-origin cross-validation is a future extension.

Batch monitoring compares new feature batches with the training profile and persists numeric mean-shift/range, missingness and categorical unseen-value diagnostics. Severity bands are operational heuristics. Monitoring does not automatically retrain, promote or roll back a model, and fairness reports are diagnostics rather than legal compliance certifications.
