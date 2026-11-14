package com.sbase.platform.bi;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DorisTenantGovernanceAuditServiceTest {
    private final JdbcTemplate postgres = mock(JdbcTemplate.class);
    private final DorisTenantGovernanceAuditService audit =
        new DorisTenantGovernanceAuditService(postgres, "analytics");

    @Test void verifiesTenantColumnAndRegistryAgainstPhysicalPostgreSqlTable() {
        when(postgres.queryForList(contains("information_schema.tables"), eq("analytics"))).thenReturn(List.of(
            Map.of("table_name","orders","table_type","BASE TABLE")));
        when(postgres.queryForObject(contains("information_schema.columns"), eq(Integer.class),
            eq("analytics"), eq("orders"))).thenReturn(1);
        when(postgres.query(contains("postgres_tenant_table_policies"), any(ResultSetExtractor.class),
            eq("analytics"), eq("orders"))).thenReturn(true);
        when(postgres.queryForMap(contains("row_count"), eq("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")))
            .thenReturn(Map.of("row_count", 100L, "invalid_rows", 0L));
        when(postgres.query(contains("verified_row_count"), any(ResultSetExtractor.class),
            eq("analytics"), eq("orders"), eq(100L))).thenReturn(true);
        when(postgres.update(contains("INSERT INTO postgres_tenant_policy_audit"), any(), any(), any(), any(), any())).thenReturn(1);

        Map<String,Object> result = audit.auditDatabase("security-review");
        assertEquals(1, result.get("tableCount"));
        assertEquals(1, result.get("verifiedShapeCount"));
        assertEquals(0, result.get("blockedCount"));
        assertEquals(true, result.get("complete"));
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> tables = (List<Map<String,Object>>) result.get("tables");
        assertEquals("VERIFIED_SHAPE", tables.get(0).get("status"));
    }

    @Test void inventoriesAllNonSystemPostgreSqlSchemas() {
        when(postgres.queryForList(contains("information_schema.schemata"))).thenReturn(List.of(
            Map.of("schema_name","analytics"), Map.of("schema_name","warehouse")));
        when(postgres.queryForList(contains("information_schema.tables"), any(Object[].class))).thenReturn(List.of());
        Map<String,Object> result = audit.auditAllDatabases("platform-security-review");
        assertEquals(2, result.get("databaseCount"));
        assertEquals(0, result.get("tableCount"));
        assertEquals(true, result.get("complete"));
    }

    @Test void rejectsUnsafeSchemaAndMissingAuditSubject() {
        assertThrows(IllegalArgumentException.class, () -> new DorisTenantGovernanceAuditService(postgres, "analytics;DROP"));
        assertThrows(IllegalArgumentException.class, () -> audit.auditDatabase(" "));
        verifyNoInteractions(postgres);
    }
}
