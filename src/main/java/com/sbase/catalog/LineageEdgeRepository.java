package com.sbase.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface LineageEdgeRepository extends JpaRepository<LineageEdgeEntity,UUID> {
    Optional<LineageEdgeEntity> findByTenantIdAndEdgeFingerprint(UUID tenantId,String edgeFingerprint);
    List<LineageEdgeEntity> findAllByTenantIdAndFromAssetIdIn(UUID tenantId,List<UUID> ids);
    List<LineageEdgeEntity> findAllByTenantIdAndToAssetIdIn(UUID tenantId,List<UUID> ids);
}