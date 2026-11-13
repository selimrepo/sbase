package com.sbase.platform.bi;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.security.access.AccessDeniedException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DorisTenantTablePolicyTest {
    private static final String REGISTRY="""
        SELECT EXISTS(SELECT 1 FROM doris_tenant_table_policies
         WHERE schema_name=? AND table_name=? AND enabled=true AND tenant_column='tenant_id' AND classification='TENANT_DATA' AND verification_status='VERIFIED' AND verified_at IS NOT NULL AND invalid_tenant_rows=0)
        """;
    private final JdbcTemplate postgres=mock(JdbcTemplate.class);
    private final JdbcTemplate doris=mock(JdbcTemplate.class);
    private final DorisTenantTablePolicy policy=new DorisTenantTablePolicy(postgres,doris,"analytics.orders,analytics.events","analytics");

    @Test void permitsOnlyRegisteredAllowListedPhysicalTenantTables() {
        when(postgres.query(eq(REGISTRY),any(ResultSetExtractor.class),eq("analytics"),eq("orders"))).thenReturn(true);
        when(doris.queryForObject(anyString(),eq(Integer.class),eq("analytics"),eq("orders"))).thenReturn(1);
        assertEquals("analytics.orders",policy.requireTenantScoped("analytics.orders"));
        verify(postgres).query(eq(REGISTRY),any(ResultSetExtractor.class),eq("analytics"),eq("orders"));
        verify(doris).queryForObject(anyString(),eq(Integer.class),eq("analytics"),eq("orders"));
    }

    @Test void failsClosedWhenTableIsNotRegistered() {
        when(postgres.query(eq(REGISTRY),any(ResultSetExtractor.class),eq("analytics"),eq("orders"))).thenReturn(false);
        assertThrows(AccessDeniedException.class,()->policy.requireTenantScoped("analytics.orders"));
        verifyNoInteractions(doris);
    }

    @Test void rejectsPhysicalTableWithoutTenantDiscriminator() {
        when(postgres.query(eq(REGISTRY),any(ResultSetExtractor.class),eq("analytics"),eq("orders"))).thenReturn(true);
        when(doris.queryForObject(anyString(),eq(Integer.class),eq("analytics"),eq("orders"))).thenReturn(0);
        assertThrows(AccessDeniedException.class,()->policy.requireTenantScoped("analytics.orders"));
    }

    @Test void rejectsTablesOutsideExplicitAllowListBeforeDatabaseAccess() {
        assertThrows(AccessDeniedException.class,()->policy.requireTenantScoped("analytics.payroll"));
        verifyNoInteractions(postgres,doris);
    }

    @Test void rejectsUnsafeIdentifiers() {
        assertThrows(IllegalArgumentException.class,()->policy.requireTenantScoped("analytics.orders; DROP TABLE users"));
        verifyNoInteractions(postgres,doris);
    }
}
