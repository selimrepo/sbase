package com.sbase.pipeline;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PipelineRepository extends JpaRepository<PipelineEntity,UUID> {
    List<PipelineEntity> findAllByTenantIdOrderByUpdatedAtDesc(UUID tenantId);
    Optional<PipelineEntity> findByIdAndTenantId(UUID id,UUID tenantId);
}
