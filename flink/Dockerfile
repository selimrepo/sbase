FROM flink:1.20.2-scala_2.12-java17

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir -p /opt/flink/lib /opt/flink/plugins/s3-fs-hadoop

# Keep the connector matrix aligned with Flink 1.20.2 and Iceberg 1.10.0.
# Every artifact is fetched from Maven Central and its published SHA-256 is verified.
ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG ICEBERG_VERSION=1.10.0
ARG FLINK_VERSION=1.20.2
ARG POSTGRESQL_VERSION=42.7.7

RUN set -eux; \
    fetch_maven() { \
      group_path="$1"; artifact="$2"; version="$3"; destination="$4"; \
      base="https://repo.maven.apache.org/maven2/${group_path}/${artifact}/${version}"; \
      file="${artifact}-${version}.jar"; \
      curl --retry 5 --retry-all-errors --fail --silent --show-error "${base}/${file}" -o "/tmp/${file}"; \
      curl --retry 5 --retry-all-errors --fail --silent --show-error "${base}/${file}.sha256" -o "/tmp/${file}.sha256"; \
      expected="$(tr -d '[:space:]' < "/tmp/${file}.sha256")"; \
      printf '%s  %s\n' "$expected" "/tmp/${file}" | sha256sum --check --status; \
      install -m 0644 "/tmp/${file}" "${destination}/${file}"; \
      rm -f "/tmp/${file}" "/tmp/${file}.sha256"; \
    }; \
    fetch_maven org/apache/flink flink-sql-connector-kafka "$KAFKA_CONNECTOR_VERSION" /opt/flink/lib; \
    fetch_maven org/apache/flink flink-json "$FLINK_VERSION" /opt/flink/lib; \
    fetch_maven org/apache/iceberg iceberg-flink-runtime-1.20 "$ICEBERG_VERSION" /opt/flink/lib; \
    fetch_maven org/apache/iceberg iceberg-aws-bundle "$ICEBERG_VERSION" /opt/flink/lib; \
    fetch_maven org/postgresql postgresql "$POSTGRESQL_VERSION" /opt/flink/lib; \
    fetch_maven org/apache/flink flink-s3-fs-hadoop "$FLINK_VERSION" /opt/flink/plugins/s3-fs-hadoop; \
    chown -R flink:flink /opt/flink

USER flink
