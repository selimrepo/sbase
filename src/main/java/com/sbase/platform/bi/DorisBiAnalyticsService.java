package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.*;

/** PostgreSQL-backed BI catalog and database activity summary. */
@Service
public class DorisBiAnalyticsService {
    private final JdbcTemplate postgres;
    private final String schema;

    public DorisBiAnalyticsService(@Qualifier("jdbcTemplate") JdbcTemplate postgres,
            @org.springframework.beans.factory.annotation.Value("${sbase.bi.schema:public}") String schema) {
        this.postgres = postgres;
        if (schema == null || !schema.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid BI schema");
        this.schema = schema;
    }

    public Map<String, Object> summary() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("database", "PostgreSQL");
        result.put("schema", schema);
        try {
            List<Map<String, Object>> raw = postgres.queryForList("""
                SELECT t.table_name,
                       COALESCE(s.n_live_tup, 0)::bigint AS estimated_rows,
                       pg_total_relation_size(format('%I.%I', t.table_schema, t.table_name)::regclass)::bigint AS storage_bytes,
                       COALESCE(c.relkind::text, 'r') AS engine
                  FROM information_schema.tables t
                  LEFT JOIN pg_stat_user_tables s
                    ON s.schemaname=t.table_schema AND s.relname=t.table_name
                  LEFT JOIN pg_class c ON c.relname=t.table_name
                  LEFT JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname=t.table_schema
                 WHERE t.table_schema=? AND t.table_type='BASE TABLE'
                 ORDER BY t.table_name
                """, schema);
            List<Map<String,Object>> tables = new ArrayList<>();
            long rows=0, bytes=0;
            for (Map<String,Object> r:raw) {
                long count=number(r.get("estimated_rows")), size=number(r.get("storage_bytes"));
                rows+=count; bytes+=size;
                Map<String,Object> item=new LinkedHashMap<>();
                item.put("name", Objects.toString(r.get("table_name"), ""));
                item.put("estimatedRows",count); item.put("storageBytes",size);
                item.put("engine","PostgreSQL"); tables.add(item);
            }
            result.put("connected",true); result.put("tableCount",tables.size());
            result.put("estimatedRows",rows); result.put("storageBytes",bytes);
            result.put("tables",tables); result.put("queryActivity",queryActivity());
        } catch (DataAccessException ex) {
            result.put("connected",false); result.put("tableCount",0);
            result.put("estimatedRows",0L); result.put("storageBytes",0L);
            result.put("tables",List.of()); result.put("queryActivity",unavailable());
            result.put("message","PostgreSQL analytics metadata could not be read");
        }
        return result;
    }

    private Map<String,Object> queryActivity() {
        try {
            Map<String,Object> r=postgres.queryForMap("""
                SELECT COALESCE(SUM(calls),0)::bigint AS query_count,
                       COALESCE(AVG(mean_exec_time),0)::double precision AS avg_query_time_ms,
                       COALESCE(SUM(shared_blks_read)*current_setting('block_size')::bigint,0)::bigint AS scanned_bytes,
                       0::bigint AS scanned_rows
                  FROM pg_stat_statements
                """);
            return Map.of("available",true,"windowHours",24,
                "queryCount",number(r.get("query_count")),
                "avgQueryTimeMs",number(r.get("avg_query_time_ms")),
                "scannedBytes",number(r.get("scanned_bytes")),
                "scannedRows",number(r.get("scanned_rows")));
        } catch (DataAccessException ex) { return unavailable(); }
    }

    private static Map<String,Object> unavailable() {
        return Map.of("available",false,"windowHours",24,"queryCount",0L,
            "avgQueryTimeMs",0L,"scannedBytes",0L,"scannedRows",0L);
    }
    private static long number(Object value) { return value instanceof Number n ? n.longValue() : 0L; }
}
