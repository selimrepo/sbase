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
    private final DorisTenantGovernanceAuditService audit=new DorisTenantGovernanceAuditService(postgres,doris,"analytics");

    @Test void inventoryFindsUnregisteredMissingAndStaleDorisTables() {
        when(doris.queryForList(anyString(),eq("analytics"))).thenReturn(List.of(
            Map.of("table_name","orders","table_type","BASE TABLE"),
            Map.of("table_name","legacy_events","table_type","BASE TABLE"),
            Map.of("table_name","raw_dump","table_type","BASE TABLE"),
            Map.of("table_name","late_arrivals","table_type","BASE TABLE")));
        when(doris.queryForObject(contains("information_schema.columns"),eq(Integer.class),eq("analytics"),eq("orders"))).thenReturn(1);
        when(doris.queryForObject(contains("information_schema.columns"),eq(Integer.class),eq("analytics"),eq("legacy_events"))).thenReturn(1);
        when(doris.queryForObject(contains("information_schema.columns"),eq(Integer.class),eq("analytics"),eq("raw_dump"))).thenReturn(0);
        when(doris.queryForObject(contains("information_schema.columns"),eq(Integer.class),eq("analytics"),eq("late_arrivals"))).thenReturn(1);
        when(postgres.query(contains("doris_tenant_table_policies"),any(ResultSetExtractor.class),eq("analytics"),eq("orders"))).thenReturn(true);
        when(postgres.query(contains("doris_tenant_table_policies"),any(ResultSetExtractor.class),eq("analytics"),eq("legacy_events"))).thenReturn(false);
        when(postgres.query(contains("doris_tenant_table_policies"),any(ResultSetExtractor.class),eq("analytics"),eq("late_arrivals"))).thenReturn(true);
        when(doris.queryForMap(contains("row_count"),eq("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")))
            .thenReturn(Map.of("row_count",100L,"invalid_rows",0L),Map.of("row_count",150L,"invalid_rows",0L));
        when(postgres.query(contains("verified_row_count"),any(ResultSetExtractor.class),eq("analytics"),eq("orders"),eq(100L))).thenReturn(true);
        when(postgres.query(contains("verified_row_count"),any(ResultSetExtractor.class),eq("analytics"),eq("late_arrivals"),eq(150L))).thenReturn(false);
        when(postgres.update(contains("INVENTORY_SCAN"),any(),any(),any(),any(),any())).thenReturn(1);

        Map<String,Object> result=audit.auditDatabase("security-review");
        assertEquals(4,result.get("tableCount"));
        assertEquals(1,result.get("verifiedShapeCount"));
        assertEquals(3,result.get("blockedCount"));
        assertEquals(false,result.get("complete"));
        @SuppressWarnings("unchecked") List<Map<String,Object>> tables=(List<Map<String,Object>>)result.get("tables");
        assertEquals(List.of("VERIFIED_SHAPE","UNREGISTERED_OR_UNVERIFIED","MISSING_TENANT_COLUMN","STALE_VERIFICATION"),
            tables.stream().map(x->x.get("status")).toList());
        verify(postgres).update(contains("SET enabled=FALSE"),eq("PENDING"),eq(0L),eq("analytics"),eq("late_arrivals"));
    }


    @Test void inventoriesEveryNonSystemDorisDatabaseVisibleToTheServiceAccount() {
        when(doris.queryForList(contains("information_schema.schemata"))).thenReturn(List.of(
            Map.of("schema_name","analytics"),Map.of("schema_name","warehouse")));
        when(doris.queryForList(contains("information_schema.tables"),any(Object[].class))).thenReturn(List.of());

        Map<String,Object> result=audit.auditAllDatabases("platform-security-review");

        assertEquals(2,result.get("databaseCount"));
        assertEquals(0,result.get("tableCount"));
        assertEquals(true,result.get("complete"));
        org.mockito.ArgumentCaptor<Object[]> schemas = org.mockito.ArgumentCaptor.forClass(Object[].class);
        verify(doris,times(2)).queryForList(contains("information_schema.tables"),schemas.capture());
        assertEquals(List.of("analytics","warehouse"),
            schemas.getAllValues().stream().map(args -> Objects.toString(args[0])).sorted().toList());
    }

    @Test void rejectsUnsafeDatabaseAndMissingAuditSubject() {
        assertThrows(IllegalArgumentException.class,()->new DorisTenantGovernanceAuditService(postgres,doris,"analytics;DROP"));
        assertThrows(IllegalArgumentException.class,()->audit.auditDatabase(" "));
        verifyNoInteractions(doris);
    }
}