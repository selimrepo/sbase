# Ingestion connector execution

The ingestion API accepts `jdbc`, `postgresql`, `kafka` (the legacy `doris` connector ID remains accepted for existing records), `debezium`, `rest` and `file` connector types. Connector IDs must reference a row in `ingestion_connectors`; the execution service injects that ID into connector options. Configure source credentials in a secret manager in production, not in request bodies.

## Durable landing and delivery semantics

Kafka and Debezium records are landed in PostgreSQL `ingestion_records` as JSONB. The uniqueness key is connector + topic/partition/offset. A batch's rows and checkpoint are committed in one PostgreSQL transaction; only after that transaction succeeds does the consumer synchronously commit Kafka offsets. A crash between database commit and Kafka offset commit causes replay, but the uniqueness constraint makes the replay idempotent. This is at-least-once delivery with idempotent landing, not a distributed exactly-once transaction.

Connector executions retry transient failures (default 3 attempts; `maxAttempts` 1–10, `retryBackoffMs` 100–60000). Exhausted runs are recorded in `ingestion_dead_letters` with source/config context and error details. Operators with `ingestion:read` can list pending entries at `GET /api/platform/ingestion/dead-letters?limit=100`; users with `ingestion:write` can replay one using `POST /api/platform/ingestion/dead-letters/{id}/replay`. Replay claims are concurrency-safe, stale claims can be reclaimed after 15 minutes, attempts and last errors are retained, and failed replays remain pending.

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

JDBC supports `loadMode=full` and `loadMode=incremental`. Incremental mode requires `watermarkColumn`, `watermarkType` (`string`, `long`, `integer`, `decimal`, `timestamp`, or `date`), `initialWatermark` for the first run, and exactly one `{watermark}` placeholder in the SELECT, for example `SELECT id, updated_at, amount FROM orders WHERE updated_at > {watermark} ORDER BY updated_at`. The watermark column must be selected. SBase loads the last committed watermark from `ingestion_checkpoints` and advances it only after the destination transaction commits. If the process crashes between destination commit and checkpoint update, the window is replayed; use a unique key/upsert strategy at the destination to make this replay idempotent. Equal watermark values can span batches/runs, so a monotonically increasing unique watermark (or composite cursor in a future connector version) is recommended. For REST, use cursor/next-page links and stable IDs. Kafka and Debezium are inherently incremental streams and retain partition offsets. File ingestion uses CSV record-number or JSONL line-number keys; use versioned paths for replacement full loads. CSV uses Apache Commons CSV with quoted commas, escaped double quotes and embedded newlines.
