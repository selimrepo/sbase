package com.sbase.platform.tenant;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
public class TenantContext {
    public UUID tenantId(Authentication authentication) {
        if (authentication instanceof JwtAuthenticationToken jwt) {
            Object value = jwt.getToken().getClaims().get("tenant_id");
            if (value == null) value = jwt.getToken().getClaims().get("tenant");
            if (value instanceof String id) {
                try { return UUID.fromString(id); }
                catch (IllegalArgumentException ignored) {
                    throw new AccessDeniedException("JWT tenant_id must be a UUID");
                }
            }
        }
        throw new AccessDeniedException("A tenant_id UUID claim is required");
    }

    public String subject(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated())
            throw new AccessDeniedException("Authentication required");
        return authentication.getName();
    }

    public void requireTenant(Authentication authentication, UUID tenantId) {
        if (!tenantId(authentication).equals(tenantId))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Resource not found");
    }
}
