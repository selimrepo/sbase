package com.sbase.platform.tenant;

import static org.junit.jupiter.api.Assertions.*;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

class TenantContextTest {
    private final TenantContext context = new TenantContext();
    @Test void resolvesTenantOnlyFromVerifiedJwtClaim() {
        UUID tenant=UUID.randomUUID();
        Jwt jwt=Jwt.withTokenValue("token").header("alg","none").claim("sub","alice")
            .claim("tenant_id",tenant.toString()).build();
        assertEquals(tenant,context.tenantId(new JwtAuthenticationToken(jwt)));
    }
    @Test void rejectsMissingTenantClaim() {
        Jwt jwt=Jwt.withTokenValue("token").header("alg","none").claim("sub","alice").build();
        assertThrows(AccessDeniedException.class,()->context.tenantId(new JwtAuthenticationToken(jwt)));
    }
    @Test void rejectsMalformedTenantClaim() {
        Jwt jwt=Jwt.withTokenValue("token").header("alg","none").claim("sub","alice").claim("tenant_id","not-a-uuid").build();
        assertThrows(AccessDeniedException.class,()->context.tenantId(new JwtAuthenticationToken(jwt)));
    }
}
