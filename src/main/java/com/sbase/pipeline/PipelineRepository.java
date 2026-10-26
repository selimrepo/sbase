package com.sbase.pipeline;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface PipelineRepository extends JpaRepository<PipelineEntity, UUID> {
    List<PipelineEntity> findAllByOrderByUpdatedAtDesc();
}