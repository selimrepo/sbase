package com.sbase.mining;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.UUID;
import com.sbase.platform.tenant.TenantContext;
import com.sbase.platform.security.PermissionService;

@RestController
@RequestMapping("/api/mining/jobs")
public class MiningJobController {
    private final MiningJobService jobs;
    private final TenantContext tenants;
    private final PermissionService permissions;
    public MiningJobController(MiningJobService jobs,TenantContext tenants,PermissionService permissions) {
        this.jobs=jobs; this.tenants=tenants; this.permissions=permissions;
    }
    @PostMapping @ResponseStatus(HttpStatus.ACCEPTED)
    public MiningJob submit(@Valid @RequestBody MiningJobRequest request,Authentication auth) {
        permissions.require(auth,"mining:write"); tenants.requireWrite(auth);
        return jobs.submit(request,tenants.tenantId(auth),tenants.subject(auth));
    }
    @GetMapping("/{id}")
    public MiningJob get(@PathVariable UUID id,Authentication auth) {
        permissions.require(auth,"mining:read");
        MiningJob job=jobs.find(id,tenants.tenantId(auth));
        if(job==null) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Mining job not found");
        return job;
    }
}
