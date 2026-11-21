# Flink connector image

The sbase-flink:1.20.2 image is built from the same Dockerfile for every JobManager and TaskManager. Connector JARs are installed into the image at build time, so cluster nodes cannot drift independently.

## Pinned compatibility matrix

| Component | Artifact | Version | Placement |
|---|---|---:|---|
| Flink runtime | apache/flink | 1.20.2 | Base image |
| Kafka SQL connector | org.apache.flink:flink-sql-connector-kafka | 3.3.0-1.20 | /opt/flink/lib |
| JSON format | org.apache.flink:flink-json | 1.20.2 | /opt/flink/lib |
| Iceberg Flink runtime | org.apache.iceberg:iceberg-flink-runtime-1.20 | 1.10.0 | /opt/flink/lib |
| Iceberg AWS bundle | org.apache.iceberg:iceberg-aws-bundle | 1.10.0 | /opt/flink/lib |
| PostgreSQL JDBC | org.postgresql:postgresql | 42.7.7 | /opt/flink/lib |
| Flink S3 filesystem | org.apache.flink:flink-s3-fs-hadoop | 1.20.2 | /opt/flink/plugins/s3-fs-hadoop |

The Docker build downloads exact Maven Central coordinates and verifies each JAR against the repository-published SHA-256 checksum before installing it. No connector is downloaded at container startup. Both Flink services use this one image build.

## Build and run

```bash
docker compose -f compose.yaml -f compose.full.yaml --profile full build flink-jobmanager flink-taskmanager
docker compose -f compose.yaml -f compose.full.yaml --profile full up -d kafka kafka-2 kafka-3 flink-jobmanager flink-taskmanager
```

To add another connector, pin its Maven coordinate in flink/Dockerfile, add it to the compatibility matrix above, and add a CI classpath assertion. Never copy a JAR manually into a running container: rebuild and roll the whole Flink cluster.

## Production notes

- Use a Kafka cluster with replication factor 3, TLS/SASL, ACLs, and a dedicated service identity; the three-broker Compose topology is for integration testing and development; production should use managed or independently operated brokers.
- Use a durable, versioned S3-compatible bucket for checkpoints and Iceberg data. Configure bucket lifecycle/versioning and separate credentials for data and checkpoints in production.
- Keep Flink, connector, Iceberg, and Java versions aligned across all nodes. Upgrade them as one tested compatibility unit.
- CI sends Kafka JSON records through a running Flink SQL job into an Iceberg table and checks visibility through the SBase Lakehouse API. This runs three Kafka brokers and two Flink TaskManagers as separate containers on one CI host. It is a distributed-process integration test, not a substitute for multi-host network-partition, broker-rebalance, failover, and recovery testing.
- Development Compose credentials are intentionally insecure. Override every credential and enable transport encryption before exposing any service.
