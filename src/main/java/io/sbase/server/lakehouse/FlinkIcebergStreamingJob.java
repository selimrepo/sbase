package io.sbase.server.lakehouse;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Starts checkpointed Flink SQL ingestion into the shared Iceberg JDBC catalog.
 * The Iceberg Flink runtime and the source connector (for example Kafka SQL)
 * must be present on the Flink cluster classpath.
 */
public final class FlinkIcebergStreamingJob {
    public record Config(String jobName, long checkpointIntervalMs, long checkpointTimeoutMs,
                         String checkpointUri, String jdbcUri, String jdbcUser, String jdbcPassword,
                         String warehouse, String s3Endpoint, String s3Region,
                         String s3AccessKey, String s3SecretKey, boolean pathStyleAccess) {
        public Config {
            Objects.requireNonNull(jobName);
            requireUri(checkpointUri, "checkpointUri");
            requireUri(jdbcUri, "jdbcUri");
            requireUri(warehouse, "warehouse");
            if(checkpointIntervalMs<1000 || checkpointTimeoutMs<checkpointIntervalMs)
                throw new IllegalArgumentException("Invalid checkpoint timing");
        }
    }

    private FlinkIcebergStreamingJob() {}

    public static TableResult submit(Config config, String insertSql) {
        Objects.requireNonNull(config);
        if(insertSql==null || !insertSql.stripLeading().toUpperCase().startsWith("INSERT INTO"))
            throw new IllegalArgumentException("Only INSERT INTO streaming statements are accepted");

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(config.checkpointUri());
        env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Duration.ofMillis(config.checkpointIntervalMs()/2));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        StreamTableEnvironment tables=StreamTableEnvironment.create(env);
        tables.executeSql("CREATE CATALOG sbase_iceberg WITH ("+
            "'type'='iceberg',"+
            "'catalog-type'='jdbc',"+
            "'uri'='"+sql(config.jdbcUri())+"',"+
            "'jdbc.user'='"+sql(config.jdbcUser())+"',"+
            "'jdbc.password'='"+sql(config.jdbcPassword())+"',"+
            "'warehouse'='"+sql(config.warehouse())+"',"+
            "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"+
            "'client.region'='"+sql(config.s3Region())+"',"+
            "'s3.endpoint'='"+sql(config.s3Endpoint())+"',"+
            "'s3.access-key-id'='"+sql(config.s3AccessKey())+"',"+
            "'s3.secret-access-key'='"+sql(config.s3SecretKey())+"',"+
            "'s3.path-style-access'='"+config.pathStyleAccess()+"')");
        return tables.executeSql(insertSql);
    }

    private static String sql(String value) {
        if(value==null) return "";
        return value.replace("'","''");
    }
    private static void requireUri(String value,String name) {
        if(value==null || value.isBlank()) throw new IllegalArgumentException(name+" is required");
    }
}
