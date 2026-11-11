# Ingestion connector execution

The ingestion API accepts `jdbc`, `doris`, `kafka`, `debezium`, `rest` and `file` connector types. Connector IDs must reference a row in `ingestion_connectors`; the execution service injects that ID into connector options. Configure source credentials in a secret manager in production, not in request bodies.

## Durable landing and delivery semantics

Kafka and Debezium records are landed in PostgreSQL `ingestion_records` as JSONB. The uniqueness key is connector + topic/partition/offset. A batch's rows and checkpoint are committed in one PostgreSQL transaction; only after that transaction succeeds does the consumer synchronously commit Kafka offsets. A crash between database commit and Kafka offset commit causes replay, but the uniqueness constraint makes the replay idempotent. This is at-least-once delivery with idempotent landing, not a distributed exactly-once transaction.

Connector executions retry transient failures (default 3 attempts; `maxAttempts` 1–10, `retryBackoffMs` 100–60000). Exhausted runs are recorded in `ingestion_dead_letters` with source/config context and error details. A dead-letter replay endpoint/operator workflow is not yet exposed; rows can be inspected and replayed by a controlled operational job.

## Kafka

POST `/api/platform/ingestion/kafka/runs` with options:
- `bootstrapServers`, `topic`
- optional `groupId`, `autoOffsetReset` (earliest/latest), `maxRecords` (1–10000), `pollTimeoutMs`
- optional retry controls `maxAttempts`, `retryBackoffMs`

The bounded run polls up to `maxRecords` or `pollTimeoutMs`, persists records to the durable landing table, then commits offsets. Use a stable connector ID and group ID across restarts.

## CDC with Debezium

Debezium runs as a Kafka Connect source connector; SBase consumes its Kafka topic using the `debezium` connector type. Configure the Debezium PostgreSQL connector with a stable `topic.prefix`, publication/slot, database credentials and table include list. Point SBase at the emitted topic. The full Debezium envelope is retained (including `before`, `after`, `op`, `source` and `ts_ms`) rather than flattening updates/deletes into ambiguous rows. SBase does not embed or administer Kafka Connect; connector lifecycle, replication slots and schema history remain Kafka Connect operational responsibilities.

## REST

Options: `url` (JSON GET endpoint), optional `bearerToken`, `maxPages`, `nextPageField` (default `next`), `idField` (default `id`). The response may be a JSON array or an object with a `data` array and optional next-page URL. Only GET is allowed.

## Files

Options: `path`, mandatory `allowedRoot`, `format` (`csv`, `jsonl`, `ndjson`), and for CSV `header` / `columns`. File paths are normalized and must remain under the configured allowed root. Rows are keyed by path and line number to make retries idempotent.

## Incremental and full loads

JDBC source queries can implement either mode today: full load with a bounded SELECT; incremental load with a source-side watermark predicate such as `WHERE updated_at > ?`. For REST, use cursor/next-page links and stable IDs. Kafka and Debezium are inherently incremental streams and retain partition offsets. File ingestion uses line-level keys; replacing a file should use a versioned path so a new full load is not mistaken for a replay. Persist the watermark in `ingestion_checkpoints` using connector ID + checkpoint key. A connector's source watermark must advance only after its corresponding target batch has committed.
