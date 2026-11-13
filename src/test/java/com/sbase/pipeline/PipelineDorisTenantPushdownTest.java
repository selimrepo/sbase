package com.sbase.pipeline;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class PipelineDorisTenantPushdownTest {
    private static final UUID TENANT=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    @Test void pushesPersistedTenantPredicateIntoDorisJdbcSourceQuery() {
        assertEquals("(SELECT * FROM analytics.orders WHERE tenant_id = '"+TENANT+"') AS sbase_tenant_source",
            PipelineExecutionService.tenantScopedDbtable("analytics.orders",TENANT));
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
