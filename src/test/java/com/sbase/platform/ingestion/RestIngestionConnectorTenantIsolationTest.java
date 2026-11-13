package com.sbase.platform.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.security.access.AccessDeniedException;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class RestIngestionConnectorTenantIsolationTest {
    private static final UUID TENANT_A=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID TENANT_B=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    @Test void acceptsOnlyRecordsExplicitlyOwnedByThePersistedConnectorTenant() {
        assertDoesNotThrow(() -> RestIngestionConnector.requireTenantPayload(
            Map.of("id",1,"tenant_id",TENANT_A.toString(),"amount",42),TENANT_A));
    }

    @Test void rejectsMissingNullAndForeignTenantDiscriminators() {
        assertThrows(AccessDeniedException.class,
            () -> RestIngestionConnector.requireTenantPayload(Map.of("id",1,"amount",42),TENANT_A));
        assertThrows(AccessDeniedException.class,
            () -> RestIngestionConnector.requireTenantPayload(new java.util.HashMap<>(Map.of("id",1,"tenant_id",TENANT_B.toString())),TENANT_A));
        Map<String,Object> nullTenant=new java.util.HashMap<>();
        nullTenant.put("id",1);nullTenant.put("tenant_id",null);
        assertThrows(AccessDeniedException.class,
            () -> RestIngestionConnector.requireTenantPayload(nullTenant,TENANT_A));
    }

    @Test void rejectsMissingConnectorTenant() {
        assertThrows(AccessDeniedException.class,
            () -> RestIngestionConnector.requireTenantPayload(Map.of("tenant_id",TENANT_A.toString()),null));
    }
}
