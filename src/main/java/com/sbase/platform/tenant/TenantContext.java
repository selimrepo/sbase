package com.sbase.platform.tenant;

import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
public class TenantContext {
    private final JdbcTemplate jdbc;
    public TenantContext() { this.jdbc=null; }
    @Autowired public TenantContext(JdbcTemplate jdbc) { this.jdbc=jdbc; }

    public UUID tenantId(Authentication authentication) {
        if (!(authentication instanceof JwtAuthenticationToken jwt))
            throw new AccessDeniedException("A verified JWT with tenant_id is required");
        Object value=jwt.getToken().getClaims().get("tenant_id");
        if(value==null) value=jwt.getToken().getClaims().get("tenant");
        if(!(value instanceof String id)) throw new AccessDeniedException("A tenant_id UUID claim is required");
        UUID tenant;
        try { tenant=UUID.fromString(id); }
        catch(IllegalArgumentException ignored) { throw new AccessDeniedException("JWT tenant_id must be a UUID"); }
        if(jdbc!=null) {
            String status=jdbc.query("SELECT status FROM tenants WHERE id=?",(rs)->rs.next()?rs.getString(1):null,tenant);
            if(!"ACTIVE".equals(status)) throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Tenant is not active");
            Integer membership=jdbc.queryForObject(
                "SELECT count(*) FROM tenant_memberships WHERE tenant_id=? AND subject=? AND status='ACTIVE'",
                Integer.class,tenant,subject(authentication));
            if(membership==null || membership==0) throw new AccessDeniedException("User is not an active member of this tenant");
        }
        return tenant;
    }
    public String subject(Authentication authentication) {
        if(authentication==null || !authentication.isAuthenticated()) throw new AccessDeniedException("Authentication required");
        return authentication.getName();
    }
    public void requireWrite(Authentication authentication) {
        UUID tenant=tenantId(authentication);
        if(jdbc==null) return;
        String role=jdbc.query("SELECT member_role FROM tenant_memberships WHERE tenant_id=? AND subject=? AND status='ACTIVE'",
            rs->rs.next()?rs.getString(1):null,tenant,subject(authentication));
        if("READ_ONLY".equals(role)) throw new AccessDeniedException("Tenant membership is read-only");
    }
    public void requireTenant(Authentication authentication,UUID tenantId) {
        if(!tenantId(authentication).equals(tenantId)) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Resource not found");
    }
}
