package com.sbase.pipeline;

import com.sbase.platform.tenant.TenantContext;
import com.sbase.platform.tenant.ResourceAclService;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.security.core.Authentication;
import java.util.List;
import java.util.UUID;

@Service
public class PipelineService {
    private final PipelineRepository repository;
    private final PipelineDefinitionValidator validator;
    private final TenantContext tenants;
    private final ResourceAclService acl;
    private final com.sbase.catalog.CatalogLineageService lineage;
    public PipelineService(PipelineRepository repository,PipelineDefinitionValidator validator,TenantContext tenants,
            ResourceAclService acl,com.sbase.catalog.CatalogLineageService lineage) {
        this.repository=repository; this.validator=validator; this.tenants=tenants; this.acl=acl; this.lineage=lineage;
    }
    private boolean admin(Authentication auth) {
        return auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("PERM_platform:admin"));
    }
    @Transactional public PipelineEntity create(PipelineRequest request,Authentication auth) {
        tenants.requireWrite(auth);
        validator.parseAndValidate(request.definitionJson());
        PipelineEntity saved=repository.save(new PipelineEntity(tenants.tenantId(auth),tenants.subject(auth),request.name(),request.description(),request.definitionJson(),request.enabled()));
        lineage.synchronizePipeline(saved);
        return saved;
    }
    @Transactional(readOnly=true) public List<PipelineEntity> list(Authentication auth) {
        UUID tenant=tenants.tenantId(auth); String subject=tenants.subject(auth);
        return repository.findAllByTenantIdOrderByUpdatedAtDesc(tenant).stream()
            .filter(p -> p.getOwnerSubject().equals(subject) || admin(auth) || canRead(tenant,p.getId(),subject)).toList();
    }
    @Transactional(readOnly=true) public PipelineEntity get(UUID id,Authentication auth) { return authorize(id,auth,false); }
    @Transactional(readOnly=true) public PipelineEntity authorize(UUID id,Authentication auth,boolean write) {
        if(write) tenants.requireWrite(auth);
        PipelineEntity p=repository.findByIdAndTenantId(id,tenants.tenantId(auth))
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,"Pipeline not found"));
        require(p,auth,write); return p;
    }
    @Transactional public PipelineEntity update(UUID id,PipelineRequest request,Authentication auth) {
        tenants.requireWrite(auth);
        validator.parseAndValidate(request.definitionJson());
        PipelineEntity p=get(id,auth); require(p,auth,true);
        p.update(request.name(),request.description(),request.definitionJson(),request.enabled());
        lineage.synchronizePipeline(p);
        return p;
    }
    @Transactional public void delete(UUID id,Authentication auth) {
        tenants.requireWrite(auth); PipelineEntity p=get(id,auth); require(p,auth,true);
        lineage.deprecateAsset(p.getTenantId(),"PIPELINE",p.getId().toString());
        repository.delete(p);
    }
    private boolean canRead(UUID tenant,UUID id,String subject) {
        try { acl.require(tenant,"PIPELINE",id,subject,false,false); return true; }
        catch(org.springframework.security.access.AccessDeniedException e) { return false; }
    }
    private void require(PipelineEntity p,Authentication auth,boolean write) {
        if(p.getOwnerSubject().equals(tenants.subject(auth)) || admin(auth)) return;
        acl.require(p.getTenantId(),"PIPELINE",p.getId(),tenants.subject(auth),write,false);
    }
}
