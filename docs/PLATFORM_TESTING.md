# SBase platform verification and load testing

The repository includes a dependency-free Python runner at \`scripts/platform_test.py\`.
It checks the running API and platform contracts, exercises the safe read paths, and
can generate concurrent load or run a long soak test. It uses only the Python standard
library; no \`pip install\` step is required.

## Start SBase

For the normal local stack:

\`\`\`bash
cp .env.example .env
docker compose up --build -d
docker compose ps
\`\`\`

For the complete stack including Cassandra, Solr, Spark and Flink:

\`\`\`bash
docker compose -f compose.yaml -f compose.full.yaml --profile full up --build -d
\`\`\`

The runner defaults to \`http://localhost:8080\`. It reads a simple \`.env\` file if
present and honors \`SBASE_BASE_URL\` and \`SBASE_HTTP_PORT\`.

## Quick checks

\`\`\`bash
python3 scripts/platform_test.py --self-test
python3 scripts/platform_test.py --scenario smoke --requests 30 --workers 3
python3 scripts/platform_test.py --scenario balanced --requests 300 --workers 12
\`\`\`

The default balanced run validates API readiness, component inventory, every
advertised operator descriptor, engine discovery, PostgreSQL read-only SQL, Spark SQL,
federated query planning, workflow graph validation, workflow/job/schedule listing,
AutoML metrics/experiments, model registry and drift history.

Optional connectors can be probed with:

\`\`\`bash
python3 scripts/platform_test.py --include-optional --scenario balanced --requests 300
\`\`\`

This adds catalog discovery, Cassandra system-table access and Flink REST checks.
Set \`SBASE_SOLR_TEST_COLLECTION\` to a real Solr collection name to enable the Solr
query probe. Optional integrations that are not running are reported as warnings,
not as failures. A pre-uploaded Flink JAR is required to test actual Flink submission;
the runner deliberately does not submit an arbitrary job.

## Persistent integration tests

The default run is read-only apart from ordinary query execution. Workflow creation,
job submission, dataset profiling and statistical drift analysis are opt-in:

\`\`\`bash
python3 scripts/platform_test.py --write-tests --requests 100 --workers 8
\`\`\`

This validates workflow creation and immutable version reads, submits a workflow,
checks idempotent run submission, polls the distributed job to a terminal state,
reads execution events, profiles a synthetic dataset and persists a PSI/KS drift run.

**These checks create persistent test records.** The current API has no delete
endpoints for workflows, jobs, drift runs or models, so the runner names test records
with a timestamp and does not pretend to clean them up. Run them against a disposable
development/test database, not a production database.

To additionally run a small Spark ML classification training job:

\`\`\`bash
python3 scripts/platform_test.py --write-tests --automl-training --requests 20 --workers 2
\`\`\`

AutoML training is intentionally separate because Spark model fitting and
cross-validation are substantially more expensive than API contract checks. It may
persist a model and artifact in the configured registry.

## Concurrent load and soak testing

The runner supports fixed request counts or duration-based runs. Each worker sends
requests concurrently; there are no automatic retries that would conceal failures or
inflate throughput.

\`\`\`bash
# 2,000 requests, 24 concurrent workers
python3 scripts/platform_test.py --scenario balanced --requests 2000 --workers 24

# 30-minute mixed read/compute soak
python3 scripts/platform_test.py --scenario compute --duration-seconds 1800 --workers 32

# 24-hour endurance run (use a dedicated non-production environment)
python3 scripts/platform_test.py --scenario balanced --duration-seconds 86400 --workers 16

# Up to one week, if explicitly intended
python3 scripts/platform_test.py --scenario balanced --duration-seconds 604800 --workers 12
\`\`\`

Scenarios:

| Scenario | Workload |
|---|---|
| \`smoke\` | Liveness, readiness, capabilities, components, operators and low-cost reads |
| \`balanced\` | Smoke coverage plus workflow/job/model/schedule/experiment and descriptor reads |
| \`compute\` | Balanced reads plus Spark SQL, PostgreSQL SELECT, workflow validation and query planning |

The load mix is read-only. It does not create workflows, train models, submit Flink JARs,
or run arbitrary SQL. PostgreSQL and Spark probes use constant \`SELECT 1\` statements.
The runner reports request count, HTTP status distribution, errors, bytes received,
throughput, mean latency and p50/p95/p99 latency globally and per route.

## Options

\`\`\`text
--base-url URL             API base URL (default: SBASE_BASE_URL or localhost)
--studio-url URL           Also check the React Studio HTTP endpoint
--env-file PATH            Read KEY=VALUE values (default: .env)
--timeout SECONDS          Per-request timeout (default: 15)
--scenario NAME            smoke | balanced | compute
--workers N                Concurrent workers (1..256)
--requests N                Fixed request count (default: 300)
--duration-seconds N       Duration mode; takes precedence over --requests
--max-error-rate FRACTION  Maximum permitted HTTP error ratio (default: 0.01)
--include-optional         Probe optional Cassandra, Solr and Flink integrations
--write-tests              Enable persistent workflow and drift integration tests
--automl-training          Also fit one small Spark ML model; requires --write-tests
--report PATH              JSON report path (default: artifacts/platform-test-report.json)
--allow-high-load          Required when workers exceed 100
\`\`\`

Examples with custom ports:

\`\`\`bash
SBASE_BASE_URL=http://127.0.0.1:8085 \
python3 scripts/platform_test.py --scenario compute --requests 500 --workers 10
\`\`\`

The JSON report contains each check, elapsed time, pass/warning/failure counts,
per-route load statistics and a sample of failed requests. A non-zero exit code means
the API did not become ready, a required contract check failed, or the load-test error
ratio exceeded the configured objective.

## Operational safety

- Start with a small smoke run and increase workers gradually.
- Do not run stress/soak tests against production or shared environments without an
  explicit capacity window and approval.
- More than 100 concurrent workers requires \`--allow-high-load\`.
- Use \`--write-tests\` only with a disposable database. It intentionally leaves
  timestamped records for later inspection.
- Long soak runs should use persistent logs/metrics and monitor PostgreSQL, Spark
  local scratch space, JVM heap, container restarts and host disk capacity separately.
- A passing HTTP load test is not a substitute for database saturation, cluster
  failover, data correctness, multi-tenant isolation or disaster-recovery testing.
