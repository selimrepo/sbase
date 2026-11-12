package com.sbase.catalog;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CatalogAssetRepository extends JpaRepository<CatalogAssetEntity,UUID> {
    Optional<CatalogAssetEntity> findByIdAndTenantId(UUID id,UUID tenantId);
    Optional<CatalogAssetEntity> findByTenantIdAndAssetTypeAndExternalKey(UUID tenantId,String assetType,String externalKey);
    List<CatalogAssetEntity> findAllByTenantIdAndAssetTypeOrderByUpdatedAtDesc(UUID tenantId,String assetType,Pageable pageable);
    List<CatalogAssetEntity> findAllByTenantIdOrderByUpdatedAtDesc(UUID tenantId,Pageable pageable);
    List<CatalogAssetEntity> findAllByTenantIdAndIdIn(UUID tenantId,List<UUID> ids);
    @Query("select a from CatalogAssetEntity a where a.tenantId=:tenant and a.assetType='TABLE' and (lower(a.physicalName)=lower(:reference) or lower(concat(coalesce(a.namespace,''),'.',coalesce(a.physicalName,'')))=lower(:reference)) order by a.updatedAt desc")
    List<CatalogAssetEntity> resolveTableReference(@Param("tenant") UUID tenantId,@Param("reference") String reference,Pageable pageable);
}