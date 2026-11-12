package com.sbase.catalog;

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
public class DatasetService {
    private final DatasetRepository repository;
    private final TenantContext tenants;
    private final ResourceAclService acl;
    private final CatalogLineageService lineage;
    public DatasetService(DatasetRepository repository,TenantContext tenants,ResourceAclService acl,CatalogLineageService lineage) {
        this.repository=repository; this.tenants=tenants; this.acl=acl; this.lineage=lineage;
    }
    private boolean admin(Authentication auth) {
        return auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("PERM_platform:admin"));
    }
    @Transactional public DatasetEntity create(DatasetRequest request,Authentication auth) {
        tenants.requireWrite(auth);
        UUID tenant=tenants.tenantId(auth);
        if(repository.findByTenantIdAndNamespaceAndTableName(tenant,request.namespace(),request.tableName()).isPresent())
            throw new ResponseStatusException(HttpStatus.CONFLICT,"Dataset table is already registered");
        DatasetEntity saved=repository.save(new DatasetEntity(tenant,tenants.subject(auth),request.name(),request.namespace(),request.tableName(),
            request.description(),request.sourceType(),normalizeSchema(request.schemaJson())));
        lineage.synchronizeDataset(saved);
        return saved;
    }
    @Transactional(readOnly=true) public List<DatasetEntity> list(Authentication auth) {
        UUID tenant=tenants.tenantId(auth); String subject=tenants.subject(auth);
        return repository.findAllByTenantIdOrderByUpdatedAtDesc(tenant).stream()
            .filter(d -> d.getOwnerSubject().equals(subject) || admin(auth) || canRead(tenant,d.getId(),subject)).toList();
    }
    @Transactional(readOnly=true) public DatasetEntity get(UUID id,Authentication auth) {
        DatasetEntity d=repository.findByIdAndTenantId(id,tenants.tenantId(auth))
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,"Dataset not found"));
        require(d,auth,false); return d;
    }
    @Transactional public DatasetEntity update(UUID id,DatasetRequest request,Authentication auth) {
        tenants.requireWrite(auth); DatasetEntity d=get(id,auth); require(d,auth,true);
        repository.findByTenantIdAndNamespaceAndTableName(d.getTenantId(),request.namespace(),request.tableName())
            .filter(other -> !other.getId().equals(id)).ifPresent(other -> {throw new ResponseStatusException(HttpStatus.CONFLICT,"Dataset table is already registered");});
        if(!d.getNamespace().equals(request.namespace()) || !d.getTableName().equals(request.tableName()))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Dataset namespace and table name are immutable");
        d.update(request.name(),request.description(),normalizeSchema(request.schemaJson()));
        lineage.synchronizeDataset(d);
        return d;
    }
    @Transactional public void delete(UUID id,Authentication auth) {
        tenants.requireWrite(auth); DatasetEntity d=get(id,auth); require(d,auth,true);
        lineage.deprecateDataset(d.getTenantId(),d.getId());
        repository.delete(d);
    }
    private boolean canRead(UUID tenant,UUID id,String subject) {
        try { acl.require(tenant,"DATASET",id,subject,false,false); return true; } catch(org.springframework.security.access.AccessDeniedException e) { return false; }
    }
    private void require(DatasetEntity d,Authentication auth,boolean write) {
        if(d.getOwnerSubject().equals(tenants.subject(auth)) || admin(auth)) return;
        acl.require(d.getTenantId(),"DATASET",d.getId(),tenants.subject(auth),write,false);
    }
    private String normalizeSchema(String schema) { return schema==null || schema.isBlank() ? "[]" : schema; }
}
