package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class TenantEnvelopeValidatorTest {
    private final UUID tenant=UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    @Test void requiresTrustedTenantHeader() {
        RecordHeaders headers=new RecordHeaders();
        headers.add(new RecordHeader("sbase-tenant-id",tenant.toString().getBytes(StandardCharsets.UTF_8)));
        assertDoesNotThrow(()->TenantEnvelopeValidator.requireTrustedHeader(headers,"sbase-tenant-id",tenant));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireTrustedHeader(headers,"sbase-tenant-id",UUID.randomUUID()));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireTrustedHeader(new RecordHeaders(),"sbase-tenant-id",tenant));
        headers.add(new RecordHeader("sbase-tenant-id",tenant.toString().getBytes(StandardCharsets.UTF_8)));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireTrustedHeader(headers,"sbase-tenant-id",tenant));
    }

    @Test void rejectsForeignTenantInNestedJsonEnvelope() {
        String json="{\"before\":{\"tenant_id\":\""+tenant+"\"},\"after\":{\"tenant_id\":\"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\"}}";
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireJsonTenantIfPresent(new ObjectMapper(),json,tenant));
    }


    @Test void rejectsMalformedOrNonObjectDebeziumPayloadsInsteadOfSkippingTenantValidation() {
        ObjectMapper mapper=new ObjectMapper();
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,"{broken",tenant));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,"[1,2,3]",tenant));
        assertThrows(SecurityException.class,()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper," ",tenant));
        assertDoesNotThrow(()->TenantEnvelopeValidator.requireValidJsonTenantIfPresent(mapper,
            "{\"before\":{\"tenant_id\":\""+tenant+"\"},\"after\":{\"tenant_id\":\""+tenant+"\"}}",tenant));
    }

    @Test void acceptsMatchingOrHeaderBoundNonJsonPayload() {
        assertDoesNotThrow(()->TenantEnvelopeValidator.requireJsonTenantIfPresent(new ObjectMapper(),
            "{\"tenant_id\":\""+tenant+"\",\"value\":42}",tenant));
        assertDoesNotThrow(()->TenantEnvelopeValidator.requireJsonTenantIfPresent(new ObjectMapper(),"binary-avro-payload",tenant));
    }
}
