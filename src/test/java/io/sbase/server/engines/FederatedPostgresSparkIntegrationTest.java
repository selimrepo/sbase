package io.sbase.server.engines;

import io.sbase.server.catalog.CatalogService;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FederatedPostgresSparkIntegrationTest {
    @Test
    void executesPostgresColumnNamedValueThroughSparkFederation() throws Exception {
        String url = System.getenv().getOrDefault("SBASE_POSTGRES_URL", "jdbc:postgresql://localhost:5432/sbase");
        String user = System.getenv().getOrDefault("SBASE_POSTGRES_USER", "sbase");
        String password = System.getenv().getOrDefault("SBASE_POSTGRES_PASSWORD", "sbase-test");
        String table = "sbase_federation_reserved_word_test";
        try (var connection = DriverManager.getConnection(url, user, password);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS public." + table + " (value INTEGER NOT NULL)");
            statement.execute("TRUNCATE TABLE public." + table);
            statement.execute("INSERT INTO public." + table + " (value) VALUES (41), (42)");
        }

        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .appName("sbase-federation-reserved-word-test")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.testing.memory", "536870912")
                .getOrCreate();
        try (CatalogService catalogs = new CatalogService();
             FederatedQueryExecutor executor = new FederatedQueryExecutor(catalogs, spark, 2)) {
            Map<String, Map<String, Object>> sources = Map.of(
                    "pg_rows", Map.of(
                            "type", "postgresql",
                            "table", table,
                            "query", "SELECT value FROM public." + table,
                            "limit", 10
                    )
            );
            Map<String, Object> result = executor.execute(
                    "SELECT value FROM pg_rows WHERE value >= 42 ORDER BY value",
                    sources, 10);
            assertEquals("SUCCEEDED", result.get("status"));
            assertEquals(1, result.get("rowCount"));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rows = (List<Map<String, Object>>) result.get("rows");
            assertEquals(42L, ((Number) rows.get(0).get("value")).longValue());
        } finally {
            spark.stop();
            try (var connection = DriverManager.getConnection(url, user, password);
                 var statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS public." + table);
            }
        }
    }
}
