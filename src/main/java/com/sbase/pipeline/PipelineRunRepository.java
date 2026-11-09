package com.sbase.pipeline;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface PipelineRunRepository extends JpaRepository<PipelineRunEntity, UUID> {
    List<PipelineRunEntity> findTop100ByPipelineIdOrderBySubmittedAtDesc(UUID pipelineId);
}
