package com.sbase.pipeline;

import com.sbase.config.PostgresProperties;
import com.sbase.postgres.PostgresClient;
import com.sbase.postgres.PostgresSparkWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
class SparkPostgresResilienceTest {
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void retriesSparkBatchesIdempotentlyAndKeepsTenantKeysIsolated() throws Exception {
        String url = postgres.getJdbcUrl();
        String user = postgres.getUsername();
        String password = postgres.getPassword();
        String table = "sbase_spark_it_" + UUID.randomUUID().toString().replace("-", "");
        UUID tenantId = UUID.randomUUID();
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        PostgresClient client = new PostgresClient(new PostgresProperties(url, user, password, "org.postgresql.Driver", "public", ""), dataSource);

        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + table + "\" (tenant_id UUID NOT NULL, id BIGINT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (tenant_id,id))");
        }

        SparkSession spark = SparkSession.builder().appName("sbase-postgres-resilience-test")
            .master("local[4]").config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.host", "127.0.0.1").getOrCreate();
        try {
            Dataset<Row> batch = spark.range(10_000)
                .selectExpr("id", "concat('payload-', cast(id as string)) as payload")
                .withColumn("tenant_id", org.apache.spark.sql.functions.lit(tenantId.toString()))
                .select("tenant_id", "id", "payload").repartition(4);

            for (int attempt = 0; attempt < 2; attempt++) {
                PostgresSparkWriter.append(batch, client, "public." + table, new String[]{"tenant_id", "id"});
            }

            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
                 var result = statement.executeQuery("SELECT COUNT(*) FROM public.\"" + table + "\" WHERE tenant_id='" + tenantId + "'")) {
                result.next();
                assertEquals(10_000L, result.getLong(1), "Retrying a Spark batch must not duplicate tenant business keys");
            }

            long sparkCount = spark.read().format("jdbc").option("url", url).option("dbtable", "public.\"" + table + "\"")
                .option("user", user).option("password", password).option("driver", "org.postgresql.Driver").load()
                .filter(org.apache.spark.sql.functions.col("tenant_id").equalTo(tenantId.toString())).count();
            assertEquals(10_000L, sparkCount);
        } finally {
            spark.stop();
            try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS public.\"" + table + "\"");
            }
        }
    }
}
