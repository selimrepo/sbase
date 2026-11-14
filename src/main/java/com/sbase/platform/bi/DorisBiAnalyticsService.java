package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/** BI catalog and workload statistics backed by the primary PostgreSQL database. */
@Service
public class DorisBiAnalyticsService {
    private final JdbcTemplate postgres;
    private final String schema;

    public DorisBiAnalyticsService(@Qualifier("jdbcTemplate") JdbcTemplate postgres,
            @Value("${sbase.postgres.database:public}") String schema) {
        this.postgres = postgres;
        if (schema == null || !schema.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid PostgreSQL analytics schema");
        this.schema = schema;
    }

    public Map<String, Object> summary() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("database", "PostgreSQL");
        result.put("schema", schema);
        try {
            List<Map<String, Object>> rawTables = postgres.queryForList("""
                SELECT t.table_name,
                       COALESCE(s.n_live_tup, 0)::bigint AS estimated_rows,
                       pg_total_relation_size(format('%I.%I', t.table_schema, t.table_name)::regclass) AS storage_bytes
                  FROM information_schema.tables t
                  LEFT JOIN pg_stat_user_tables s
                    ON s.schemaname=t.table_schema AND s.relname=t.table_name
                 WHERE t.table_schema=? AND t.table_type='BASE TABLE'
                 ORDER BY t.table_name
                """, schema);

            List<Map<String, Object>> tables = new ArrayList<>();
            long estimatedRows = 0;
            long storageBytes = 0;
            for (Map<String, Object> row : rawTables) {
                long rows = number(row.get("estimated_rows"));
                long bytes = number(row.get("storage_bytes"));
                estimatedRows += rows;
                storageBytes += bytes;
                Map<String, Object> table = new LinkedHashMap<>();
                table.put("name", Objects.toString(row.get("table_name"), ""));
                table.put("estimatedRows", rows);
                table.put("storageBytes", bytes);
                table.put("engine", "PostgreSQL");
                tables.add(table);
            }
            result.put("connected", true);
            result.put("tableCount", tables.size());
            result.put("estimatedRows", estimatedRows);
            result.put("storageBytes", storageBytes);
            result.put("tables", tables);
            result.put("queryActivity", queryActivity());
        } catch (DataAccessException ex) {
            result.put("connected", false);
            result.put("tableCount", 0);
            result.put("estimatedRows", 0L);
            result.put("storageBytes", 0L);
            result.put("tables", List.of());
            result.put("queryActivity", unavailableQueryActivity());
            result.put("message", "PostgreSQL analytics metadata could not be read");
        }
        return result;
    }

    private Map<String, Object> queryActivity() {
        try {
            Map<String, Object> row = postgres.queryForMap("""
                SELECT COALESCE(SUM(calls),0) AS query_count,
                       COALESCE(SUM(total_exec_time) / NULLIF(SUM(calls),0),0) AS avg_query_time_ms,
                       COALESCE(SUM(shared_blks_read) * current_setting('block_size')::bigint,0) AS read_bytes,
                       COALESCE(SUM(rows),0) AS returned_rows
                  FROM pg_stat_statements
                """);
            return Map.of("available", true, "windowHours", 0,
                    "queryCount", number(row.get("query_count")),
                    "avgQueryTimeMs", number(row.get("avg_query_time_ms")),
                    "scannedBytes", number(row.get("read_bytes")),
                    "scannedRows", number(row.get("returned_rows")));
        } catch (DataAccessException | IllegalStateException ex) {
            // pg_stat_statements is optional; catalog and table statistics remain available.
            return unavailableQueryActivity();
        }
    }

    private static Map<String, Object> unavailableQueryActivity() {
        return Map.of("available", false, "windowHours", 0,
                "queryCount", 0L, "avgQueryTimeMs", 0L,
                "scannedBytes", 0L, "scannedRows", 0L);
    }

    private static long number(Object value) {
        return value instanceof Number n ? n.longValue() : 0L;
    }
}
