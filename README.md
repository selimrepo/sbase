# SBase — Advanced Data Science & Data Mining Hub

SBase is a Spring Boot platform for data engineering, distributed data mining, machine learning and analytical serving.

## Architecture

- **PostgreSQL + Spring Data JPA** — transactional metadata: users, datasets, experiments, model registry, job state, permissions and audit data.
- **Apache Spark** — distributed SQL, ETL, DataFrames, MLlib and batch/stream processing.
- **Apache Mahout** — additional scalable linear algebra and machine-learning algorithms, exposed through algorithm adapters.
- **Apache Doris** — analytical warehouse for facts, aggregates, feature tables and BI queries.

PostgreSQL is the system of record. Doris is the analytical store. Do not use Doris for JPA entities or transactional job metadata.

## Requirements

Docker Engine and Docker Compose plugin. Allocate at least 8 GB RAM for the complete local stack; Spark workers and Doris need additional memory for meaningful workloads.

## Run

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
docker compose logs -f sbase
```

Services:
- SBase API: http://localhost:8080
- Actuator health: http://localhost:8080/actuator/health
- Spark master UI: http://localhost:8081
- Doris FE web UI: http://localhost:8030
- PostgreSQL: localhost:5432
- Doris MySQL protocol: localhost:9030

On first Doris startup, register the backend if it is not automatically registered:

```sql
SHOW BACKENDS;
ALTER SYSTEM ADD BACKEND "doris-be:9050";
CREATE DATABASE IF NOT EXISTS analytics;
```

## Mining API

Submit an asynchronous mining job:

```http
POST /api/mining/jobs
Content-Type: application/json

{
  "sourceTable": "analytics.customer_features",
  "outputTable": "analytics.customer_clusters",
  "operation": "kmeans",
  "k": 4,
  "maxIterations": 30
}
```

Poll `GET /api/mining/jobs/{id}`. Job metadata is persisted in PostgreSQL through Spring Data JPA.

Initial Spark operations include descriptive statistics, data profiling and K-means clustering. Model output publishing to Doris is implemented using Spark JDBC writes; create the target Doris table with a schema matching the DataFrame before publishing.

## Configuration

See `.env.example`. Spring Boot uses the standard `spring.datasource.*` PostgreSQL configuration. Doris has its own independent JDBC URL and credentials under `sbase.doris.*`; it is accessed through a dedicated client and does not replace or interfere with the JPA datasource.

## Development

```bash
gradle clean test
gradle bootRun
```

Spark defaults to `local[*]` outside Compose. In Compose the application connects to the Spark standalone master.

## Next modules

Dataset catalog and ingestion connectors, Mahout algorithm adapters, experiment tracking, model registry/versioning, feature engineering, durable distributed job scheduling, access control, and dashboard/BI APIs.
