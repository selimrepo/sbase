package com.sbase.platform.tenant;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ResourceAclService {
    private final JdbcTemplate jdbc;
    public ResourceAclService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void require(UUID tenantId, String type, UUID resourceId, String subject, boolean write, boolean admin) {
        if (admin) return;
        Integer allowed = jdbc.query("""
            SELECT count(*) FROM resource_access_grants
             WHERE tenant_id=? AND resource_type=? AND resource_id=? AND subject=?
               AND (permission='ADMIN' OR permission='WRITE' OR (?=false AND permission='READ'))
            """, rs -> rs.next() ? rs.getInt(1) : 0, tenantId, type, resourceId, subject, write);
        if (allowed == 0) throw new AccessDeniedException("Resource ACL denies access");
    }

    @Transactional
    public void grant(UUID tenantId, String type, UUID resourceId, String subject, String permission, String grantedBy) {
        if (!permission.matches("READ|WRITE|ADMIN")) throw new IllegalArgumentException("Invalid ACL permission");
        jdbc.update("""
            INSERT INTO resource_access_grants(id,tenant_id,resource_type,resource_id,subject,permission,granted_by)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(tenant_id,resource_type,resource_id,subject)
            DO UPDATE SET permission=EXCLUDED.permission, granted_by=EXCLUDED.granted_by, created_at=now()
            """, UUID.randomUUID(), tenantId, type, resourceId, subject, permission, grantedBy);
    }

    @Transactional
    public void revoke(UUID tenantId, String type, UUID resourceId, String subject) {
        jdbc.update("DELETE FROM resource_access_grants WHERE tenant_id=? AND resource_type=? AND resource_id=? AND subject=?",
            tenantId, type, resourceId, subject);
    }
}
