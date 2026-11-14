package com.sbase.platform.bi;

import com.sbase.platform.security.PermissionService;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/** Platform-admin-only inventory and verification endpoint for the physical PostgreSQL catalog. */
@RestController
@RequestMapping("/api/platform/postgres/tenant-governance")
public class DorisTenantGovernanceController {
    private final DorisTenantGovernanceAuditService audit;
    private final PermissionService permissions;

    public DorisTenantGovernanceController(DorisTenantGovernanceAuditService audit, PermissionService permissions) {
        this.audit=audit;
        this.permissions=permissions;
    }

    @PostMapping("/audit")
    @ResponseStatus(HttpStatus.OK)
    public Map<String,Object> auditConfiguredDatabase(Authentication authentication) {
        permissions.require(authentication,"platform:admin");
        return audit.auditDatabase(permissions.subject(authentication));
    }

    @PostMapping("/audit-all")
    @ResponseStatus(HttpStatus.OK)
    public Map<String,Object> auditAllDatabases(Authentication authentication) {
        permissions.require(authentication,"platform:admin");
        return audit.auditAllDatabases(permissions.subject(authentication));
    }
}
