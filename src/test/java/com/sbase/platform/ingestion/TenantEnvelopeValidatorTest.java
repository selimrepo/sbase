package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class TenantEnvelopeValidatorTest {
    private static final UUID TENANT_A=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID TENANT_B=UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private final ObjectMapper mapper=new ObjectMapper();

    @Test void kafkaHeaderMustBePresentUniqueAndOwnedByConnectorTenant() {
        Headers valid=new RecordHeaders().add("sbase-tenant-id",TENANT_A.toString().getBytes(StandardCharsets.UTF_8));
        assertDoesNotThrow(()->TenantEnvelopeValidator.requireTrustedHeader(valid,"sbase-tenant-id",TENANT_A));

        RecordHeaders missing=new RecordHeaders();
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireTrustedHeader(missing,"sbase-tenant-id",TENANT_A));

        Headers foreign=new RecordHeaders().add("sbase-tenant-id",TENANT_B.toString().getBytes(StandardCharsets.UTF_8));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireTrustedHeader(foreign,"sbase-tenant-id",TENANT_A));

        Headers duplicate=new RecordHeaders()
            .add("sbase-tenant-id",TENANT_A.toString().getBytes(StandardCharsets.UTF_8))
            .add("sbase-tenant-id",TENANT_A.toString().getBytes(StandardCharsets.UTF_8));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireTrustedHeader(duplicate,"sbase-tenant-id",TENANT_A));
    }

    @Test void kafkaJsonPayloadCannotSmuggleForeignTenantInNestedBeforeAfterData() {
        assertDoesNotThrow(()->TenantEnvelopeValidator.requireJsonTenantIfPresent(mapper,
            "{\"after\":{\"tenant_id\":\""+TENANT_A+"\"}}",TENANT_A));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireJsonTenantIfPresent(mapper,
            "{\"payload\":{\"after\":{\"tenant_id\":\""+TENANT_B+"\"}}}",TENANT_A));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireJsonTenantIfPresent(mapper,
            "{\"tenant_id\":null}",TENANT_A));
    }

    @Test void debeziumRequiresWellFormedJsonAndRejectsForeignNestedTenant() {
        assertDoesNotThrow(()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,
            "{\"before\":null,\"after\":{\"tenant_id\":\""+TENANT_A+"\"},\"op\":\"c\"}",TENANT_A));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,
            "{\"after\":{\"tenant_id\":\""+TENANT_B+"\"}}",TENANT_A));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,"not-json",TENANT_A));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,"[]",TENANT_A));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,"{}",null));
    }
}
