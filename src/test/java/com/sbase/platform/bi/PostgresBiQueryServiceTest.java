package com.sbase.platform.bi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresBiQueryServiceTest {
    private final JdbcTemplate jdbc=mock(JdbcTemplate.class);
    private final PostgresTenantTablePolicy policy=mock(PostgresTenantTablePolicy.class);
    private final PostgresBiQueryService service=new PostgresBiQueryService(jdbc,policy);
    private final UUID tenantA=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    @BeforeEach void register(){when(policy.requireTenantScoped("analytics.orders")).thenReturn("analytics.orders");}

    @Test void injectsAuthenticatedTenantPredicateAndDefaultLimit(){
        when(jdbc.queryForList("SELECT id FROM analytics.orders WHERE tenant_id::text = ? LIMIT 1000",tenantA.toString())).thenReturn(List.of(Map.of("id",1)));
        assertEquals(1,service.query(tenantA,"SELECT id FROM analytics.orders").size());
        verify(jdbc).queryForList("SELECT id FROM analytics.orders WHERE tenant_id::text = ? LIMIT 1000",tenantA.toString());
    }
    @Test void tenantPredicateIsAddedOutsideCallerWhereExpression(){
        when(jdbc.queryForList("SELECT id FROM analytics.orders WHERE (status = 'OPEN' OR status = 'PENDING') AND tenant_id::text = ? ORDER BY id LIMIT 25",tenantA.toString())).thenReturn(List.of(Map.of("id",1)));
        service.query(tenantA,"SELECT id FROM analytics.orders WHERE status = 'OPEN' OR status = 'PENDING' ORDER BY id LIMIT 25");
        verify(jdbc).queryForList("SELECT id FROM analytics.orders WHERE (status = 'OPEN' OR status = 'PENDING') AND tenant_id::text = ? ORDER BY id LIMIT 25",tenantA.toString());
    }
    @Test void rejectsUnscopedLegacyApiAndMissingTenant(){
        assertThrows(IllegalStateException.class,()->service.query("SELECT id FROM analytics.orders"));
        assertThrows(IllegalArgumentException.class,()->service.query(null,"SELECT id FROM analytics.orders"));
        verifyNoInteractions(jdbc,policy);
    }
    @Test void rejectsCallerSuppliedTenantPredicateAndTenantOverride(){
        assertThrows(IllegalArgumentException.class,()->service.query(tenantA,"SELECT id FROM analytics.orders WHERE tenant_id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'"));
        verifyNoInteractions(jdbc);
        verifyNoInteractions(policy);
    }
    @Test void rejectsUnionSubqueriesUnlistedTablesAndExcessiveLimits(){
        assertThrows(IllegalArgumentException.class,()->service.query(tenantA,"SELECT id FROM analytics.orders UNION SELECT id FROM secrets"));
        assertThrows(IllegalArgumentException.class,()->service.query(tenantA,"SELECT id FROM analytics.orders WHERE id IN (SELECT id FROM secrets)"));
        when(policy.requireTenantScoped("secrets")).thenThrow(new org.springframework.security.access.AccessDeniedException("not allow-listed"));
        assertThrows(org.springframework.security.access.AccessDeniedException.class,()->service.query(tenantA,"SELECT id FROM secrets"));
        assertThrows(IllegalArgumentException.class,()->service.query(tenantA,"SELECT id FROM analytics.orders LIMIT 10001"));
        verifyNoInteractions(jdbc);
    }
    @Test void refusesTablesWithoutAnActivePolicy(){
        when(policy.requireTenantScoped("analytics.orders")).thenThrow(new org.springframework.security.access.AccessDeniedException("not registered"));
        assertThrows(org.springframework.security.access.AccessDeniedException.class,()->service.query(tenantA,"SELECT id FROM analytics.orders"));
        verifyNoInteractions(jdbc);
    }
    @Test void scopesTenantBeforeGroupAndOrderClauses(){
        when(jdbc.queryForList("SELECT region, SUM(amount) AS total FROM analytics.orders WHERE tenant_id::text = ? GROUP BY region ORDER BY region LIMIT 25",tenantA.toString())).thenReturn(List.of(Map.of("region","EU","total",10)));
        service.query(tenantA,"SELECT region, SUM(amount) AS total FROM analytics.orders GROUP BY region ORDER BY region LIMIT 25");
        verify(jdbc).queryForList("SELECT region, SUM(amount) AS total FROM analytics.orders WHERE tenant_id::text = ? GROUP BY region ORDER BY region LIMIT 25",tenantA.toString());
    }
}
