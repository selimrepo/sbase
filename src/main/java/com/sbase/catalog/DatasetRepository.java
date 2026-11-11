package com.sbase.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DatasetRepository extends JpaRepository<DatasetEntity,UUID> {
    List<DatasetEntity> findAllByTenantIdOrderByUpdatedAtDesc(UUID tenantId);
    Optional<DatasetEntity> findByIdAndTenantId(UUID id,UUID tenantId);
    Optional<DatasetEntity> findByTenantIdAndNamespaceAndTableName(UUID tenantId,String namespace,String tableName);
}
