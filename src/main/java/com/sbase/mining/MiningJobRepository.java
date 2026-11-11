package com.sbase.mining;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
import java.util.UUID;

public interface MiningJobRepository extends JpaRepository<MiningJobEntity,UUID> {
    Optional<MiningJobEntity> findByIdAndTenantId(UUID id,UUID tenantId);
}
