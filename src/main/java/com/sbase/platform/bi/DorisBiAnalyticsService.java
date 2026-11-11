package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Reads BI-facing OLAP statistics from Doris. PostgreSQL remains the source
 * for platform/control-plane metadata and is deliberately not used here.
 */
@Service
public class DorisBiAnalyticsService {
    private final JdbcTemplate doris;
    private final String database;

    public DorisBiAnalyticsService(
            @Qualifier("dorisJdbcTemplate") JdbcTemplate doris,
            @Value("${sbase.doris.database:analytics}") String database) {
        this.doris = doris;
        this.database = database;
    }

    public Map<String, Object> summary() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("database", database);
        try {
            List<Map<String, Object>> rawTables = doris.queryForList("""
                    SELECT TABLE_NAME AS table_name,
                           COALESCE(TABLE_ROWS, 0) AS estimated_rows,
                           COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0) AS storage_bytes,
                           COALESCE(ENGINE, 'UNKNOWN') AS engine
                    FROM information_schema.tables
                    WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                    """, database);

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
                table.put("engine", Objects.toString(row.get("engine"), "UNKNOWN"));
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
            result.put("message", "Doris analytics metadata could not be read");
        }
        return result;
    }

    private Map<String, Object> queryActivity() {
        try {
            Map<String, Object> row = doris.queryForMap("""
                    SELECT COUNT(*) AS query_count,
                           COALESCE(AVG(query_time), 0) AS avg_query_time_ms,
                           COALESCE(SUM(scan_bytes), 0) AS scanned_bytes,
                           COALESCE(SUM(scan_rows), 0) AS scanned_rows
                    FROM information_schema.query_audit_log
                    WHERE start_time >= NOW() - INTERVAL 24 HOUR
                      AND is_query = 1
                    """);
            return Map.of(
                    "available", true,
                    "windowHours", 24,
                    "queryCount", number(row.get("query_count")),
                    "avgQueryTimeMs", number(row.get("avg_query_time_ms")),
                    "scannedBytes", number(row.get("scanned_bytes")),
                    "scannedRows", number(row.get("scanned_rows")));
        } catch (DataAccessException ex) {
            // Query audit logging can be disabled or unavailable in a Doris
            // deployment. Keep table-level OLAP statistics usable regardless.
            return unavailableQueryActivity();
        }
    }

    private static Map<String, Object> unavailableQueryActivity() {
        return Map.of("available", false, "windowHours", 24,
                "queryCount", 0L, "avgQueryTimeMs", 0L,
                "scannedBytes", 0L, "scannedRows", 0L);
    }

    private static long number(Object value) {
        return value instanceof Number n ? n.longValue() : 0L;
    }
}
