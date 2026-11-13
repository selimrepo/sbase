package com.sbase.platform.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.security.access.AccessDeniedException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class FileIngestionConnectorTenantIsolationTest {
    private static final UUID TENANT_A=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID TENANT_B=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    @Test void acceptsOnlyRowsExplicitlyOwnedByTheTenantDirectoryOwner() {
        assertDoesNotThrow(() -> FileIngestionConnector.requireTenantPayload(
            Map.of("tenant_id",TENANT_A.toString(),"id","order-1"),TENANT_A));
    }

    @Test void rejectsMissingNullAndForeignTenantIdsForCsvAndJsonlRows() {
        assertThrows(AccessDeniedException.class,
            () -> FileIngestionConnector.requireTenantPayload(Map.of("id","order-1"),TENANT_A));
        assertThrows(AccessDeniedException.class,
            () -> FileIngestionConnector.requireTenantPayload(Map.of("tenant_id",TENANT_B.toString()),TENANT_A));
        Map<String,Object> nullTenant=new HashMap<>();
        nullTenant.put("tenant_id",null);
        assertThrows(AccessDeniedException.class,
            () -> FileIngestionConnector.requireTenantPayload(nullTenant,TENANT_A));
    }
}
