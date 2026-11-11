package com.sbase.platform.audit;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import com.sbase.platform.security.PermissionService;
import com.sbase.platform.tenant.TenantContext;

@RestController
@RequestMapping("/api/platform/audit-events")
public class AuditEventController {
    private final JdbcTemplate jdbc;
    private final PermissionService permissions;
    private final TenantContext tenants;
    public AuditEventController(JdbcTemplate jdbc,PermissionService permissions,TenantContext tenants) {
        this.jdbc=jdbc; this.permissions=permissions; this.tenants=tenants;
    }
    public record AuditEventView(long id,Instant occurredAt,String subject,String action,String resourceType,
            String resourceId,String outcome,String requestId,String sourceIp,String userAgent,String details) {}
    @GetMapping
    public List<AuditEventView> list(@RequestParam(defaultValue="100") int limit,
            @RequestParam(required=false) Instant before,Authentication auth) {
        permissions.require(auth,"audit:read");
        int safeLimit=Math.max(1,Math.min(500,limit));
        UUID tenant=tenants.tenantId(auth);
        String sql="""
            SELECT id,occurred_at,subject,action,resource_type,resource_id,outcome,request_id,
                   host(source_ip) AS source_ip,user_agent,details::text AS details
              FROM audit_events
             WHERE tenant_id=? AND (? IS NULL OR occurred_at < ?)
             ORDER BY occurred_at DESC,id DESC LIMIT ?
            """;
        return jdbc.query(sql,(rs,n)->new AuditEventView(rs.getLong("id"),rs.getTimestamp("occurred_at").toInstant(),
            rs.getString("subject"),rs.getString("action"),rs.getString("resource_type"),rs.getString("resource_id"),
            rs.getString("outcome"),rs.getString("request_id"),rs.getString("source_ip"),rs.getString("user_agent"),
            rs.getString("details")),tenant,before,before,safeLimit);
    }
}
