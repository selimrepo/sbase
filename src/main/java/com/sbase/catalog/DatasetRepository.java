package com.sbase.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DatasetRepository extends JpaRepository<DatasetEntity, UUID> {
    List<DatasetEntity> findAllByOrderByUpdatedAtDesc();
    Optional<DatasetEntity> findByNamespaceAndTableName(String namespace, String tableName);
}