package io.sbase.server.lakehouse;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
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
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(config.checkpointUri()));
        env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.checkpointIntervalMs()/2);
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

    public static TableResult submit(Config config,String sourceDdl,String insertSql) {
        Objects.requireNonNull(sourceDdl,"sourceDdl");
        if(!sourceDdl.stripLeading().toUpperCase().startsWith("CREATE TABLE"))
            throw new IllegalArgumentException("Only CREATE TABLE source DDL is accepted");
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(config.checkpointIntervalMs(),CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(config.checkpointUri()));
        env.getCheckpointConfig().setCheckpointTimeout(config.checkpointTimeoutMs());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.checkpointIntervalMs()/2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        StreamTableEnvironment tables=StreamTableEnvironment.create(env);
        tables.executeSql("CREATE CATALOG sbase_iceberg WITH ("+
            "'type'='iceberg','catalog-type'='jdbc','uri'='"+sql(config.jdbcUri())+
            "','jdbc.user'='"+sql(config.jdbcUser())+"','jdbc.password'='"+sql(config.jdbcPassword())+
            "','warehouse'='"+sql(config.warehouse())+"','io-impl'='org.apache.iceberg.aws.s3.S3FileIO"+
            "','client.region'='"+sql(config.s3Region())+"','s3.endpoint'='"+sql(config.s3Endpoint())+
            "','s3.access-key-id'='"+sql(config.s3AccessKey())+"','s3.secret-access-key'='"+sql(config.s3SecretKey())+
            "','s3.path-style-access'='"+config.pathStyleAccess()+"')");
        tables.executeSql(sourceDdl);
        return tables.executeSql(insertSql);
    }

    /** Flink REST entry point. Connector/runtime JARs must be installed in the cluster image. */
    public static void main(String[] args) throws Exception {
        if(args.length!=1) throw new IllegalArgumentException("Expected one JSON job configuration argument");
        com.fasterxml.jackson.databind.ObjectMapper json=new com.fasterxml.jackson.databind.ObjectMapper();
        String payload=new String(java.util.Base64.getUrlDecoder().decode(args[0]),java.nio.charset.StandardCharsets.UTF_8);
        java.util.Map<?,?> values=json.readValue(payload,java.util.Map.class);
        String checkpoint=Objects.toString(values.get("checkpointUri"),"");
        String sourceDdl=Objects.toString(values.get("sourceDdl"),"");
        String insertSql=Objects.toString(values.get("insertSql"),"");
        Config config=new Config("SBase Iceberg streaming ingestion",
            Long.parseLong(System.getenv().getOrDefault("SBASE_FLINK_CHECKPOINT_INTERVAL_MS","30000")),
            Long.parseLong(System.getenv().getOrDefault("SBASE_FLINK_CHECKPOINT_TIMEOUT_MS","120000")),
            checkpoint,System.getenv("SBASE_ICEBERG_JDBC_URL"),System.getenv("SBASE_ICEBERG_JDBC_USER"),
            System.getenv("SBASE_ICEBERG_JDBC_PASSWORD"),System.getenv("SBASE_ICEBERG_WAREHOUSE"),
            System.getenv("S3_ENDPOINT"),System.getenv().getOrDefault("S3_REGION","us-east-1"),
            System.getenv("S3_ACCESS_KEY"),System.getenv("S3_SECRET_KEY"),
            Boolean.parseBoolean(System.getenv().getOrDefault("S3_PATH_STYLE_ACCESS","true")));
        submit(config,sourceDdl,insertSql).await();
    }

    private static String sql(String value) {
        if(value==null) return "";
        return value.replace("'","''");
    }
    private static void requireUri(String value,String name) {
        if(value==null || value.isBlank()) throw new IllegalArgumentException(name+" is required");
    }
}
