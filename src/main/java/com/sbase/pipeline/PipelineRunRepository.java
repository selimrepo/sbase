package com.sbase.pipeline;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;
import java.util.UUID;

public interface PipelineRunRepository extends JpaRepository<PipelineRunEntity, UUID> {
    List<PipelineRunEntity> findTop100ByPipelineIdOrderBySubmittedAtDesc(UUID pipelineId);

    List<PipelineRunEntity> findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByNextAttemptAtAsc(String status, java.time.Instant now);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE pipeline_runs SET status='RUNNING', started_at=now(), attempt_count=attempt_count+1, next_attempt_at=NULL, error_message=NULL WHERE id=:id AND status='QUEUED'", nativeQuery = true)
    int claimQueued(@Param("id") UUID id);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE pipeline_runs SET status='QUEUED' WHERE id=:id AND status='RETRY_WAIT' AND next_attempt_at<=now()", nativeQuery = true)
    int claimDueRetry(@Param("id") UUID id);
}
