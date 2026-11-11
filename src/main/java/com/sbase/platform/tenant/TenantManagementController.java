package com.sbase.platform.tenant;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import com.sbase.platform.security.PermissionService;

@RestController
@RequestMapping("/api/platform/tenants")
public class TenantManagementController {
    private final JdbcTemplate jdbc;
    private final PermissionService permissions;
    public TenantManagementController(JdbcTemplate jdbc,PermissionService permissions){this.jdbc=jdbc;this.permissions=permissions;}
    public record TenantRequest(String slug,String displayName){}
    public record MemberRequest(String subject,String role){}
    public record TenantView(UUID id,String slug,String displayName,String status,Instant createdAt){}
    public record MemberView(String subject,String role,String status,Instant createdAt){}

    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public TenantView create(@RequestBody TenantRequest request,Authentication auth){
        permissions.require(auth,"tenants:admin");
        if(request.slug()==null||!request.slug().matches("[a-z0-9][a-z0-9-]{1,78}[a-z0-9]"))
            throw new org.springframework.web.server.ResponseStatusException(HttpStatus.BAD_REQUEST,"slug must be 3-80 lowercase letters, digits or hyphens");
        if(request.displayName()==null||request.displayName().isBlank()||request.displayName().length()>160)
            throw new org.springframework.web.server.ResponseStatusException(HttpStatus.BAD_REQUEST,"displayName is required (max 160)");
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",id,request.slug(),request.displayName().trim());
        jdbc.update("INSERT INTO tenant_memberships(tenant_id,subject,member_role,status,invited_by) VALUES(?,?,'OWNER','ACTIVE',?)",
            id,auth.getName(),auth.getName());
        return getById(id);
    }
    @GetMapping public List<TenantView> list(Authentication auth){
        permissions.require(auth,"tenants:admin");
        return jdbc.query("SELECT id,slug,display_name,status,created_at FROM tenants ORDER BY created_at DESC",
            (rs,n)->new TenantView(rs.getObject("id",UUID.class),rs.getString("slug"),rs.getString("display_name"),rs.getString("status"),rs.getTimestamp("created_at").toInstant()));
    }
    @PostMapping("/{id}/members") @ResponseStatus(HttpStatus.CREATED)
    public MemberView addMember(@PathVariable UUID id,@RequestBody MemberRequest request,Authentication auth){
        permissions.require(auth,"tenants:admin");
        if(request.subject()==null||request.subject().isBlank()||request.subject().length()>200)
            throw new org.springframework.web.server.ResponseStatusException(HttpStatus.BAD_REQUEST,"subject is required (max 200)");
        String role=request.role()==null?"MEMBER":request.role().toUpperCase(java.util.Locale.ROOT);
        if(!role.matches("ADMIN|MEMBER|READ_ONLY")) throw new org.springframework.web.server.ResponseStatusException(HttpStatus.BAD_REQUEST,"role must be ADMIN, MEMBER or READ_ONLY");
        jdbc.update("""
            INSERT INTO tenant_memberships(tenant_id,subject,member_role,status,invited_by)
            VALUES(?,?,?,'ACTIVE',?)
            ON CONFLICT(tenant_id,subject) DO UPDATE SET member_role=EXCLUDED.member_role,status='ACTIVE',
                invited_by=EXCLUDED.invited_by,updated_at=now()
            """,id,request.subject().trim(),role,auth.getName());
        return jdbc.queryForObject("SELECT subject,member_role,status,created_at FROM tenant_memberships WHERE tenant_id=? AND subject=?",
            (rs,n)->new MemberView(rs.getString("subject"),rs.getString("member_role"),rs.getString("status"),rs.getTimestamp("created_at").toInstant()),id,request.subject().trim());
    }
    @GetMapping("/{id}/members")
    public List<MemberView> members(@PathVariable UUID id,Authentication auth){
        permissions.require(auth,"tenants:admin");
        return jdbc.query("SELECT subject,member_role,status,created_at FROM tenant_memberships WHERE tenant_id=? ORDER BY created_at",
            (rs,n)->new MemberView(rs.getString("subject"),rs.getString("member_role"),rs.getString("status"),rs.getTimestamp("created_at").toInstant()),id);
    }
    @DeleteMapping("/{id}/members/{subject}") @ResponseStatus(HttpStatus.NO_CONTENT)
    public void suspendMember(@PathVariable UUID id,@PathVariable String subject,Authentication auth){
        permissions.require(auth,"tenants:admin");
        int changed=jdbc.update("UPDATE tenant_memberships SET status='SUSPENDED',updated_at=now() WHERE tenant_id=? AND subject=? AND member_role<>'OWNER'",id,subject);
        if(changed==0) throw new org.springframework.web.server.ResponseStatusException(HttpStatus.NOT_FOUND,"Member not found or owner cannot be suspended here");
    }
    @PostMapping("/{id}/suspend") @ResponseStatus(HttpStatus.NO_CONTENT)
    public void suspend(@PathVariable UUID id,Authentication auth){
        permissions.require(auth,"tenants:admin");
        int changed=jdbc.update("UPDATE tenants SET status='SUSPENDED' WHERE id=? AND status='ACTIVE'",id);
        if(changed==0) throw new org.springframework.web.server.ResponseStatusException(HttpStatus.NOT_FOUND,"Active tenant not found");
    }
    @PostMapping("/{id}/activate") @ResponseStatus(HttpStatus.NO_CONTENT)
    public void activate(@PathVariable UUID id,Authentication auth){
        permissions.require(auth,"tenants:admin");
        int changed=jdbc.update("UPDATE tenants SET status='ACTIVE' WHERE id=? AND status='SUSPENDED'",id);
        if(changed==0) throw new org.springframework.web.server.ResponseStatusException(HttpStatus.NOT_FOUND,"Suspended tenant not found");
    }
    private TenantView getById(UUID id){
        return jdbc.queryForObject("SELECT id,slug,display_name,status,created_at FROM tenants WHERE id=?",
            (rs,n)->new TenantView(rs.getObject("id",UUID.class),rs.getString("slug"),rs.getString("display_name"),rs.getString("status"),rs.getTimestamp("created_at").toInstant()),id);
    }
}
