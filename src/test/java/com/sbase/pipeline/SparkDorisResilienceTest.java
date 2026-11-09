package com.sbase.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkDorisResilienceTest {
    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    @EnabledIfEnvironmentVariable(named = "SBASE_DORIS_TEST_JDBC_URL", matches = ".+")
    void loadsReadsAndRetriesDuplicateUniqueKeysWithoutGrowingTheTarget() throws Exception {
        String url = System.getenv("SBASE_DORIS_TEST_JDBC_URL");
        String user = System.getenv().getOrDefault("SBASE_DORIS_TEST_USERNAME", "root");
        String password = System.getenv().getOrDefault("SBASE_DORIS_TEST_PASSWORD", "");
        String table = "sbase_it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        awaitAliveBackend(url, user, password);
        try (var connection = DriverManager.getConnection(url, user, password);
             var statement = connection.createStatement()) {
            String createTableSql = "CREATE TABLE " + table +
                " (id BIGINT NOT NULL, payload VARCHAR(128)) UNIQUE KEY(id) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES " +
                "(\"replication_num\"=\"1\", \"storage_medium\"=\"HDD\")";
            createTableWithBackendRetry(statement, createTableSql);
        }

        SparkSession spark = SparkSession.builder().appName("sbase-doris-resilience-test")
            .master("local[4]").config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.host", "127.0.0.1").getOrCreate();
        try {
            Dataset<Row> batch = spark.range(10_000)
                .selectExpr("id", "concat('payload-', cast(id as string)) as payload")
                .repartition(1); // One Doris bucket: avoid four concurrent writers contending for one tablet.
            for (int attempt = 0; attempt < 2; attempt++) {
                System.out.println("Spark-Doris append attempt " + (attempt + 1) + "/2");
                batch.write().format("jdbc").option("url", url).option("dbtable", table)
                    .option("user", user).option("password", password)
                    .option("driver", "com.mysql.cj.jdbc.Driver").option("batchsize", "10000")
                    .mode("append").save();
            }
            try (var connection = DriverManager.getConnection(url, user, password);
                 var statement = connection.createStatement();
                 var result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
                result.next();
                assertEquals(10_000L, result.getLong(1),
                    "Repeating a Spark JDBC append to a Doris UNIQUE KEY table must not duplicate entities");
            }
            assertEquals(10_000L, spark.read().format("jdbc").option("url", url).option("dbtable", table)
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

    /**
     * FE recovery can temporarily leave tablet scheduling without backend path
     * information. Retry only that transient placement error; all other DDL
     * failures must fail immediately.
     */
    private static void createTableWithBackendRetry(java.sql.Statement statement, String sql) throws Exception {
        SQLException lastFailure = null;
        for (int attempt = 1; attempt <= 60; attempt++) {
            try {
                statement.execute(sql);
                return;
            } catch (SQLException exception) {
                // Doris 2.1.x may expose this DDL placement failure as vendor
                // code 1105 (or 2 depending on the MySQL protocol path). The
                // stable discriminator is the Doris error message, not JDBC's
                // vendor-code mapping.
                if (exception.getMessage() == null
                        || !exception.getMessage().contains("Failed to find enough backend")) {
                    throw exception;
                }
                lastFailure = exception;
                if (attempt < 60) {
                    if (attempt == 1 || attempt % 12 == 0) {
                        System.err.println("Doris has not scheduled a tablet yet; retry " + attempt + "/60");
                    }
                    Thread.sleep(5_000);
                }
            }
        }
        throw new IllegalStateException(
            "Doris FE did not make the registered BE schedulable for tablet placement after 5 minutes",
            lastFailure);
    }

    /**
     * Wait until the FE has received the BE's storage-path report. Alive=true
     * is insufficient: Doris may still have no schedulable disk capacity.
     */
    private static void awaitAliveBackend(String url, String user, String password) throws Exception {
        long deadline = System.nanoTime() + java.time.Duration.ofMinutes(3).toNanos();
        SQLException lastFailure = null;
        String lastBackendState = "no backend rows";
        int consecutiveHealthyObservations = 0;
        while (System.nanoTime() < deadline) {
            try (var connection = DriverManager.getConnection(url, user, password);
                 var statement = connection.createStatement();
                 ResultSet backends = statement.executeQuery("SHOW BACKENDS")) {
                ResultSetMetaData metadata = backends.getMetaData();
                int hostColumn = -1;
                int aliveColumn = -1;
                int decommissionedColumn = -1;
                int availableCapacityColumn = -1;
                int totalCapacityColumn = -1;
                for (int column = 1; column <= metadata.getColumnCount(); column++) {
                    String label = metadata.getColumnLabel(column);
                    if ("Host".equalsIgnoreCase(label)) hostColumn = column;
                    if ("Alive".equalsIgnoreCase(label)) aliveColumn = column;
                    if ("SystemDecommissioned".equalsIgnoreCase(label)) decommissionedColumn = column;
                    if ("AvailCapacity".equalsIgnoreCase(label)) availableCapacityColumn = column;
                    if ("TotalCapacity".equalsIgnoreCase(label)) totalCapacityColumn = column;
                }
                if (hostColumn < 0 || aliveColumn < 0 || decommissionedColumn < 0
                        || availableCapacityColumn < 0 || totalCapacityColumn < 0) {
                    throw new SQLException("SHOW BACKENDS must expose Host, Alive, SystemDecommissioned, "
                        + "AvailCapacity and TotalCapacity columns");
                }

                boolean schedulableBackendFound = false;
                while (backends.next()) {
                    String host = backends.getString(hostColumn);
                    String available = backends.getString(availableCapacityColumn);
                    String total = backends.getString(totalCapacityColumn);
                    boolean alive = Boolean.parseBoolean(backends.getString(aliveColumn));
                    boolean decommissioned = Boolean.parseBoolean(backends.getString(decommissionedColumn));
                    lastBackendState = "host=" + host + ", alive=" + alive + ", decommissioned=" + decommissioned
                        + ", availableCapacity=" + available + ", totalCapacity=" + total;
                    if (alive && !decommissioned && hasPositiveCapacity(available) && hasPositiveCapacity(total)) {
                        schedulableBackendFound = true;
                        break;
                    }
                }
                consecutiveHealthyObservations = schedulableBackendFound
                    ? consecutiveHealthyObservations + 1 : 0;
                lastFailure = null;
                if (consecutiveHealthyObservations >= 3) return;
            } catch (SQLException exception) {
                consecutiveHealthyObservations = 0;
                lastFailure = exception;
            }
            Thread.sleep(5_000);
        }
        throw new IllegalStateException(
            "Doris FE recovered, but no Alive, non-decommissioned backend with registered disk capacity "
                + "was observed for 3 consecutive checks within 3 minutes. Last backend state: " + lastBackendState,
            lastFailure);
    }

    private static boolean hasPositiveCapacity(String capacity) {
        if (capacity == null || capacity.isBlank()) return false;
        String numericPart = capacity.trim().split("\\s+")[0].replace(",", "");
        try {
            return Double.parseDouble(numericPart) > 0;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }
}
