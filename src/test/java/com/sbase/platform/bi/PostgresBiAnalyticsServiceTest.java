package com.sbase.platform.bi;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PostgresBiAnalyticsServiceTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final PostgresBiAnalyticsService service = new PostgresBiAnalyticsService(jdbc, "public");

    @Test
    void summarizesPostgreSqlTablesAndOptionalQueryStatistics() {
        when(jdbc.queryForList(anyString(), any(Object[].class))).thenReturn(List.of(
            Map.of("table_name","events","estimated_rows",1200L,"storage_bytes",4096L),
            Map.of("table_name","orders","estimated_rows",300L,"storage_bytes",2048L)));
        when(jdbc.queryForMap(anyString())).thenReturn(Map.of(
            "query_count",42L,"avg_query_time_ms",18L,"read_bytes",8192L,"returned_rows",1500L));
        Map<String,Object> summary=service.summary();
        assertEquals(true,summary.get("connected"));
        assertEquals("PostgreSQL",summary.get("database"));
        assertEquals("public",summary.get("schema"));
        assertEquals(2,summary.get("tableCount"));
        assertEquals(1500L,summary.get("estimatedRows"));
        assertEquals(6144L,summary.get("storageBytes"));
        Map<?,?> activity=(Map<?,?>)summary.get("queryActivity");
        assertEquals(true,activity.get("available"));
        assertEquals(42L,activity.get("queryCount"));
        assertEquals(18L,activity.get("avgQueryTimeMs"));
    }

    @Test
    void keepsCatalogStatisticsWhenPgStatStatementsIsUnavailable() {
        when(jdbc.queryForList(anyString(), any(Object[].class))).thenReturn(List.of(
            Map.of("table_name","events","estimated_rows",9L,"storage_bytes",512L)));
        when(jdbc.queryForMap(anyString())).thenThrow(new DataAccessResourceFailureException("extension disabled"));
        Map<String,Object> summary=service.summary();
        assertEquals(true,summary.get("connected"));
        assertEquals(1,summary.get("tableCount"));
        assertEquals(9L,summary.get("estimatedRows"));
        assertEquals(false,((Map<?,?>)summary.get("queryActivity")).get("available"));
    }

    @Test
    void reportsPostgreSqlUnavailableWithoutInventingAHealthyResult() {
        when(jdbc.queryForList(anyString(), any(Object[].class)))
            .thenThrow(new DataAccessResourceFailureException("PostgreSQL unavailable"));
        Map<String,Object> summary=service.summary();
        assertEquals(false,summary.get("connected"));
        assertEquals(0,summary.get("tableCount"));
        assertEquals("PostgreSQL analytics metadata could not be read",summary.get("message"));
    }
}
