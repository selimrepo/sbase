package com.sbase.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface CatalogSchemaVersionRepository extends JpaRepository<CatalogSchemaVersionEntity,UUID> {
    boolean existsByTenantIdAndTableAssetIdAndSchemaFingerprint(UUID tenantId,UUID tableAssetId,String schemaFingerprint);
    List<CatalogSchemaVersionEntity> findTop100ByTenantIdAndTableAssetIdOrderByObservedAtDesc(UUID tenantId,UUID tableAssetId);
}