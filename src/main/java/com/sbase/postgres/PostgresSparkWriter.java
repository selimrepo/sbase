package com.sbase.postgres;

import com.sbase.doris.DorisClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Idempotent Spark-to-PostgreSQL writer. Spark loads a run-scoped unlogged
 * staging table; a single PostgreSQL INSERT .. ON CONFLICT merges it into the
 * governed target. Retrying a batch cannot create duplicate business keys.
 */
public final class PostgresSparkWriter {
    private PostgresSparkWriter() { }

    public static void append(Dataset<Row> dataset, DorisClient postgres, String governedTable, String[] requestedKeys) {
        Objects.requireNonNull(dataset, "dataset");
        Objects.requireNonNull(postgres, "postgres");
        String[] parts = governedTable.split("\\.", 2);
        if (parts.length != 2 || !safe(parts[0]) || !safe(parts[1]))
            throw new IllegalArgumentException("A schema-qualified PostgreSQL table is required");
        String schema = parts[0], table = parts[1];
        String[] keys = requestedKeys == null || requestedKeys.length == 0
                ? primaryKey(postgres, schema, table) : requestedKeys.clone();
        if (keys.length == 0) throw new IllegalArgumentException("PostgreSQL target must have a primary key or explicit idempotencyColumns");
        for (String key : keys) if (!safe(key)) throw new IllegalArgumentException("Invalid PostgreSQL idempotency column");
        if (Arrays.stream(dataset.columns()).noneMatch("tenant_id"::equalsIgnoreCase))
            throw new org.springframework.security.access.AccessDeniedException("Spark output must retain tenant_id");
        String keyList = Arrays.stream(keys).map(PostgresSparkWriter::quote).collect(Collectors.joining(","));
        String target = quote(schema) + "." + quote(table);
        String stageName = "sbase_stage_" + UUID.randomUUID().toString().replace("-", "");
        String stage = quote(schema) + "." + quote(stageName);
        boolean created = false;
        try {
            try (Connection connection = postgres.openConnection(); Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                statement.execute("CREATE UNLOGGED TABLE " + stage + " (LIKE " + target + " INCLUDING DEFAULTS)");
                connection.commit();
                created = true;
            }
            dataset.write().format("jdbc")
                    .option("url", postgres.jdbcUrl())
                    .option("dbtable", stage)
                    .option("user", postgres.username())
                    .option("password", postgres.password())
                    .option("driver", "org.postgresql.Driver")
                    .option("batchsize", "2000")
                    .option("numPartitions", "4")
                    .mode("append").save();

            try (Connection connection = postgres.openConnection(); Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                statement.executeUpdate("INSERT INTO " + target + " SELECT * FROM " + stage
                        + " ON CONFLICT (" + keyList + ") DO NOTHING");
                statement.execute("DROP TABLE " + stage);
                connection.commit();
                created = false;
            }
        } catch (Exception failure) {
            if (created) {
                try (Connection connection = postgres.openConnection(); Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE IF EXISTS " + stage);
                } catch (SQLException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException("Idempotent Spark write to PostgreSQL failed", failure);
        }
    }

    private static String[] primaryKey(DorisClient postgres, String schema, String table) {
        try (Connection connection = postgres.openConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            Map<Short,String> columns = new TreeMap<>();
            try (ResultSet keys = metadata.getPrimaryKeys(connection.getCatalog(), schema, table)) {
                while (keys.next()) columns.put(keys.getShort("KEY_SEQ"), keys.getString("COLUMN_NAME"));
            }
            return columns.values().toArray(String[]::new);
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot inspect PostgreSQL primary key for " + schema + "." + table, failure);
        }
    }

    private static boolean safe(String identifier) {
        return identifier != null && identifier.matches("[A-Za-z_][A-Za-z0-9_]*");
    }
    private static String quote(String identifier) { return "\"" + identifier + "\""; }
}
