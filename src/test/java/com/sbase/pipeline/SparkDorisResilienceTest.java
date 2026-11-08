package com.sbase.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.DriverManager;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.sql.SQLException;

class SparkDorisResilienceTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "SBASE_DORIS_TEST_JDBC_URL", matches = ".+")
    void jdbcEndpointIsUnavailableDuringDorisBackendOutage() {
        String url = System.getenv("SBASE_DORIS_TEST_JDBC_URL");
        String user = System.getenv().getOrDefault("SBASE_DORIS_TEST_USERNAME", "root");
        String password = System.getenv().getOrDefault("SBASE_DORIS_TEST_PASSWORD", "");
        assertThrows(SQLException.class, () -> DriverManager.getConnection(url, user, password),
            "The Doris SQL endpoint must reject connections during the injected outage");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "SBASE_DORIS_TEST_JDBC_URL", matches = ".+")
    void loadsReadsAndRetriesDuplicateUniqueKeysWithoutGrowingTheTarget() throws Exception {
        String url = System.getenv("SBASE_DORIS_TEST_JDBC_URL");
        String user = System.getenv().getOrDefault("SBASE_DORIS_TEST_USERNAME", "root");
        String password = System.getenv().getOrDefault("SBASE_DORIS_TEST_PASSWORD", "");
        String table = "sbase_it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        try (var connection = DriverManager.getConnection(url, user, password);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + table +
                " (id BIGINT NOT NULL, payload VARCHAR(128)) UNIQUE KEY(id) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        }

        SparkSession spark = SparkSession.builder().appName("sbase-doris-resilience-test")
            .master("local[4]").config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.host", "127.0.0.1").getOrCreate();
        try {
            Dataset<Row> batch = spark.range(100_000)
                .selectExpr("id", "concat('payload-', cast(id as string)) as payload")
                .repartition(4);
            for (int attempt = 0; attempt < 2; attempt++) {
                batch.write().format("jdbc").option("url", url).option("dbtable", table)
                    .option("user", user).option("password", password)
                    .option("driver", "com.mysql.cj.jdbc.Driver").option("batchsize", "2000")
                    .mode("append").save();
            }
            try (var connection = DriverManager.getConnection(url, user, password);
                 var statement = connection.createStatement();
                 var result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
                result.next();
                assertEquals(100_000L, result.getLong(1),
                    "Repeating a Spark JDBC append to a Doris UNIQUE KEY table must not duplicate entities");
            }
            assertEquals(100_000L, spark.read().format("jdbc").option("url", url).option("dbtable", table)
                .option("user", user).option("password", password)
                .option("driver", "com.mysql.cj.jdbc.Driver").load().count());
        } finally {
            spark.stop();
            try (var connection = DriverManager.getConnection(url, user, password);
                 var statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }
}
