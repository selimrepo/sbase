package com.sbase.platform.bi;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DorisBiAnalyticsServiceTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final DorisBiAnalyticsService service = new DorisBiAnalyticsService(jdbc, "analytics");

    @Test
    void summarizesPhysicalDorisTablesAndQueryAuditMetrics() {
        when(jdbc.queryForList(anyString(), any(Object[].class))).thenReturn(List.of(
                Map.of("table_name", "events", "estimated_rows", 1200L,
                        "storage_bytes", 4096L, "engine", "OLAP"),
                Map.of("table_name", "orders", "estimated_rows", 300L,
                        "storage_bytes", 2048L, "engine", "OLAP")));
        when(jdbc.queryForMap(anyString())).thenReturn(Map.of(
                "query_count", 42L, "avg_query_time_ms", 18L,
                "scanned_bytes", 8192L, "scanned_rows", 1500L));

        Map<String, Object> summary = service.summary();

        assertEquals(true, summary.get("connected"));
        assertEquals("analytics", summary.get("database"));
        assertEquals(2, summary.get("tableCount"));
        assertEquals(1500L, summary.get("estimatedRows"));
        assertEquals(6144L, summary.get("storageBytes"));
        assertEquals(2, ((List<?>) summary.get("tables")).size());
        Map<?, ?> activity = (Map<?, ?>) summary.get("queryActivity");
        assertEquals(true, activity.get("available"));
        assertEquals(42L, activity.get("queryCount"));
        assertEquals(18L, activity.get("avgQueryTimeMs"));
    }

    @Test
    void keepsTableStatisticsWhenDorisQueryAuditLogIsUnavailable() {
        when(jdbc.queryForList(anyString(), any(Object[].class))).thenReturn(List.of(
                Map.of("table_name", "events", "estimated_rows", 9L,
                        "storage_bytes", 512L, "engine", "OLAP")));
        when(jdbc.queryForMap(anyString()))
                .thenThrow(new DataAccessResourceFailureException("query audit log disabled"));

        Map<String, Object> summary = service.summary();

        assertEquals(true, summary.get("connected"));
        assertEquals(1, summary.get("tableCount"));
        assertEquals(9L, summary.get("estimatedRows"));
        assertEquals(false, ((Map<?, ?>) summary.get("queryActivity")).get("available"));
    }

    @Test
    void reportsDorisUnavailableWithoutInventingZeroAsAHealthyResult() {
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenThrow(new DataAccessResourceFailureException("Doris unavailable"));

        Map<String, Object> summary = service.summary();

        assertEquals(false, summary.get("connected"));
        assertEquals(0, summary.get("tableCount"));
        assertEquals("Doris analytics metadata could not be read", summary.get("message"));
    }
}
