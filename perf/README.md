# SBase performance and resilience test profiles

These k6 profiles are intended for an isolated staging environment with production-like data volume and the same proxy, database pool, Kafka, Doris and object-store topology as the release candidate.

## Prerequisites

Install k6, provide the staging base URL and a scoped test JWT:

```bash
export SBASE_URL=https://sbase-staging.example
export SBASE_TOKEN='<short-lived-test-token>'
```

## Profiles

- API smoke: `k6 run perf/k6/smoke.js` — 2-minute, 5-VU readiness and dataset API baseline.
- Stress: `k6 run perf/k6/stress.js` — ramps 20 → 100 → 200 VUs and back down.
- Soak: `VUS=25 DURATION=24h k6 run perf/k6/soak.js` — 24-hour constant-load stability profile.

The scripts use GET requests only and are not a substitute for ingestion throughput or Spark/Doris data-plane benchmarks. Run separate workload generators for Kafka publish/consume, connector full/incremental loads, pipeline DAGs, Doris analytical queries and S3 artifact upload/download. Never run stress or soak profiles against customer production data.

## Release acceptance gates

Record the environment size, dataset cardinality, broker/BE/FE counts and exact commit SHA with every run. A release candidate should meet all of the following:

- 24-hour soak with less than 1% HTTP errors and no monotonically increasing heap, connection-pool or disk usage.
- p95 API latency below 1.2 seconds and p99 below 2.5 seconds for the included read workload.
- 200-VU stress profile with less than 2% HTTP errors and p95 below 1.5 seconds.
- No tenant-crossing reads/writes when two tenants run concurrently.
- Broker loss with min ISR=2, consumer restart, rebalance and checkpoint recovery without acknowledged data loss.
- One Doris FE loss without write/query outage beyond the agreed SLO; one BE loss with replicas remaining available and tablet recovery completing.
- PostgreSQL point-in-time or snapshot restore, Doris snapshot restore and MinIO version recovery verified from a separate failure domain.
- At least one 24-hour DLQ/replay and checkpoint recovery exercise.

Capture Prometheus/Grafana dashboards during every test: request rate, p95/p99, error rate, JVM heap/GC, Hikari active/pending, PostgreSQL locks/replication lag, Kafka lag/ISR, Doris query latency/tablet health/compaction, MinIO disk and object counts. Attach reports to the release record.
