package com.sbase.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface CatalogColumnRepository extends JpaRepository<CatalogColumnEntity,UUID> {
    List<CatalogColumnEntity> findAllByTenantIdAndTableAssetIdOrderByOrdinalPosition(UUID tenantId,UUID tableAssetId);
    List<CatalogColumnEntity> findAllByTenantIdAndTableAssetIdAndCurrentTrueOrderByOrdinalPosition(UUID tenantId,UUID tableAssetId);
    boolean existsByTenantIdAndTableAssetIdAndOrdinalPositionAndCurrentTrue(UUID tenantId,UUID tableAssetId,int ordinalPosition);
}