package com.sbase.mining;

import java.time.Instant;
import java.util.UUID;

public record MiningJob(
        UUID id,
        String operation,
        String sourceTable,
        String outputTable,
        MiningJobStatus status,
        Instant submittedAt,
        Instant startedAt,
        Instant finishedAt,
        String resultSummary,
        String error
) {
    static MiningJob from(MiningJobEntity entity) {
        return new MiningJob(entity.getId(), entity.getOperation(), entity.getSourceTable(),
                entity.getOutputTable(), entity.getStatus(), entity.getSubmittedAt(),
                entity.getStartedAt(), entity.getFinishedAt(), entity.getResultSummary(),
                entity.getErrorMessage());
    }
}
