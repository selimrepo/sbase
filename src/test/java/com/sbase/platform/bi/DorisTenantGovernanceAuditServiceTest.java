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
    private final JdbcTemplate postgres=mock(JdbcTemplate.class);
    private final JdbcTemplate doris=mock(JdbcTemplate.class);
    private final DorisTenantGovernanceAuditService audit=
        new DorisTenantGovernanceAuditService(postgres,doris,"analytics");

    @Test void inventoryIncludesEveryPhysicalTableAndMarksUnregisteredOrUnsafeTablesBlocked() {
        when(doris.queryForList(anyString(),eq("analytics"))).thenReturn(List.of(
            Map.of("table_name","orders"),Map.of("table_name","legacy_events"),Map.of("table_name","raw_dump")));
        when(doris.queryForObject(contains("information_schema.columns"),eq(Integer.class),eq("analytics"),eq("orders"))).thenReturn(1);
        when(doris.queryForObject(contains("information_schema.columns"),eq(Integer.class),eq("analytics"),eq("legacy_events"))).thenReturn(1);
        when(doris.queryForObject(contains("information_schema.columns"),eq(Integer.class),eq("analytics"),eq("raw_dump"))).thenReturn(0);
        when(postgres.query(contains("doris_tenant_table_policies"),any(ResultSetExtractor.class),eq("analytics"),eq("orders"))).thenReturn(true);
        when(postgres.query(contains("doris_tenant_table_policies"),any(ResultSetExtractor.class),eq("analytics"),eq("legacy_events"))).thenReturn(false);
        when(doris.queryForMap(contains("row_count"),eq("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")))
            .thenReturn(Map.of("row_count",100L,"invalid_rows",0L));
        when(postgres.update(contains("INVENTORY_SCAN"),any(),any(),any(),any(),any())).thenReturn(1);

        Map<String,Object> result=audit.auditDatabase("security-review");
        assertEquals(3,result.get("tableCount"));
        assertEquals(1,result.get("verifiedShapeCount"));
        assertEquals(2,result.get("blockedCount"));
        assertEquals(false,result.get("complete"));
        @SuppressWarnings("unchecked") List<Map<String,Object>> tables=(List<Map<String,Object>>)result.get("tables");
        assertEquals(List.of("VERIFIED_SHAPE","UNREGISTERED_OR_UNVERIFIED","MISSING_TENANT_COLUMN"),
            tables.stream().map(x->x.get("status")).toList());
    }

    @Test void rejectsUnsafeDatabaseAndMissingAuditSubject() {
        assertThrows(IllegalArgumentException.class,()->new DorisTenantGovernanceAuditService(postgres,doris,"analytics;DROP"));
        assertThrows(IllegalArgumentException.class,()->audit.auditDatabase(" "));
        verifyNoInteractions(doris);
    }
}
