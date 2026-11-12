package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.Header;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

final class TenantEnvelopeValidator {
    private TenantEnvelopeValidator() {}

    static void requireTrustedHeader(Headers headers,String headerName,UUID tenantId) {
        if(headers==null || tenantId==null) throw new SecurityException("Tenant identity header is required");
        java.util.Iterator<Header> values=headers.headers(headerName).iterator();
        if(!values.hasNext()) throw new SecurityException("Kafka message tenant header is missing");
        Header header=values.next();
        if(values.hasNext() || !tenantId.toString().equals(new String(header.value(),StandardCharsets.UTF_8)))
            throw new SecurityException("Kafka message tenant header is duplicated or does not match the connector tenant");
    }

    static void requireJsonTenantIfPresent(ObjectMapper mapper,String json,UUID tenantId) {
        if(json==null || json.isBlank()) return;
        try {
            JsonNode root=mapper.readTree(json);
            if(root!=null) inspect(root,tenantId);
        } catch(SecurityException ex) {
            throw ex;
        } catch(Exception ignored) {
            // Non-JSON payloads are permitted only when the trusted Kafka header
            // is supplied and the connector's broker ACL protects that header.
        }
    }

    private static void inspect(JsonNode node,UUID tenantId) {
        if(node.isObject()) {
            if(node.has("tenant_id") && (node.get("tenant_id").isNull() ||
                    !tenantId.toString().equals(node.get("tenant_id").asText())))
                throw new SecurityException("Kafka payload contains a foreign tenant_id");
            node.elements().forEachRemaining(child->inspect(child,tenantId));
        } else if(node.isArray()) {
            node.elements().forEachRemaining(child->inspect(child,tenantId));
        }
    }
}
