package com.sbase.pipeline;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class PipelinePostgresTenantPushdownTest {
    private static final UUID TENANT=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    @Test void pushesPersistedTenantPredicateIntoDorisJdbcSourceQuery() {
        assertEquals("(SELECT * FROM analytics.orders WHERE tenant_id = '"+TENANT+"') AS sbase_tenant_source",
            PipelineExecutionService.tenantScopedDbtable("analytics.orders",TENANT));
    }

    @Test void requiresTenantInDorisUniqueKeysToPreventCrossTenantUpsertCollisions() {
        assertDoesNotThrow(() -> PipelineExecutionService.requireTenantScopedIdempotencyKeys(
            new String[]{"tenant_id","order_id"}));
        assertDoesNotThrow(() -> PipelineExecutionService.requireTenantScopedIdempotencyKeys(
            new String[]{"ORDER_ID","TENANT_ID"}));
        assertThrows(org.springframework.security.access.AccessDeniedException.class,
            () -> PipelineExecutionService.requireTenantScopedIdempotencyKeys(new String[]{"order_id"}));
        assertThrows(org.springframework.security.access.AccessDeniedException.class,
            () -> PipelineExecutionService.requireTenantScopedIdempotencyKeys(new String[0]));
    }

    @Test void rejectsMissingTenantAndUntrustedTableIdentifiersBeforeBuildingJdbcQuery() {
        assertThrows(IllegalArgumentException.class,
            () -> PipelineExecutionService.tenantScopedDbtable("analytics.orders",null));
        assertThrows(IllegalArgumentException.class,
            () -> PipelineExecutionService.tenantScopedDbtable("analytics.orders WHERE 1=1",TENANT));
        assertThrows(IllegalArgumentException.class,
            () -> PipelineExecutionService.tenantScopedDbtable("analytics.orders; DROP TABLE users",TENANT));
    }
}
