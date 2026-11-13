package com.sbase.platform.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.security.access.AccessDeniedException;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class DurableRecordSinkTenantBoundaryTest {
    private final UUID tenantA=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private final UUID tenantB=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    @Test void stampsTenantFromPersistedConnectorForSourcesWithoutTenantColumn() {
        var normalized=DurableRecordSink.normalizeTenantRecord(tenantA,
            new DurableRecordSink.Envelope("file:1",Map.of("name","record")));
        assertEquals(tenantA.toString(),normalized.payload().get("tenant_id"));
        assertEquals("record",normalized.payload().get("name"));
    }

    @Test void rejectsForeignTenantAndMissingPersistedTenant() {
        assertThrows(AccessDeniedException.class,()->DurableRecordSink.normalizeTenantRecord(tenantA,
            new DurableRecordSink.Envelope("rest:1",Map.of("tenant_id",tenantB.toString()))));
        assertThrows(AccessDeniedException.class,()->DurableRecordSink.normalizeTenantRecord(null,
            new DurableRecordSink.Envelope("file:2",Map.of("name","record"))));
        assertThrows(AccessDeniedException.class,()->DurableRecordSink.normalizeTenantRecord(tenantA,
            new DurableRecordSink.Envelope("file:3",java.util.Collections.singletonMap("tenant_id",null))));
    }
}