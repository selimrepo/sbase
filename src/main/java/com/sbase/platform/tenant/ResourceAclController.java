package com.sbase.platform.tenant;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import com.sbase.platform.security.PermissionService;

@RestController
@RequestMapping("/api/access-grants")
public class ResourceAclController {
    private final ResourceAclService acl;
    private final TenantContext tenants;
    private final PermissionService permissions;
    public ResourceAclController(ResourceAclService acl,TenantContext tenants,PermissionService permissions) {
        this.acl=acl; this.tenants=tenants; this.permissions=permissions;
    }
    public record GrantRequest(String resourceType,UUID resourceId,String subject,String permission) {}
    @PostMapping @ResponseStatus(HttpStatus.NO_CONTENT)
    public void grant(@RequestBody GrantRequest request,Authentication auth) {
        permissions.require(auth,"resources:admin");
        String type=request.resourceType()==null?"":request.resourceType().toUpperCase(java.util.Locale.ROOT);
        if(!type.matches("DATASET|PIPELINE")) throw new org.springframework.web.server.ResponseStatusException(HttpStatus.BAD_REQUEST,"resourceType must be DATASET or PIPELINE");
        acl.grant(tenants.tenantId(auth),type,request.resourceId(),request.subject(),request.permission().toUpperCase(java.util.Locale.ROOT),tenants.subject(auth));
    }
    @DeleteMapping
    public void revoke(@RequestParam String resourceType,@RequestParam UUID resourceId,@RequestParam String subject,Authentication auth) {
        permissions.require(auth,"resources:admin");
        String type=resourceType.toUpperCase(java.util.Locale.ROOT);
        if(!type.matches("DATASET|PIPELINE")) throw new org.springframework.web.server.ResponseStatusException(HttpStatus.BAD_REQUEST,"resourceType must be DATASET or PIPELINE");
        acl.revoke(tenants.tenantId(auth),type,resourceId,subject);
    }
}
