FROM flink:1.20.2-scala_2.12-java17
# Place connector JARs (for example flink-sql-connector-kafka) in ./flink/lib.
# Iceberg runtime and S3FileIO are included in the uploaded SBase job artifact.
COPY lib/ /opt/flink/lib/
