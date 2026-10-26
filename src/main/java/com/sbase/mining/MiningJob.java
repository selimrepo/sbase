package com.sbase.mining;

import java.time.Instant;
import java.util.UUID;

public record MiningJob(
        UUID id,
        String operation,
        MiningJobStatus status,
        Instant submittedAt,
        Instant startedAt,
        Instant finishedAt,
        String resultSummary,
        String error
) { }
