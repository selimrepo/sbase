package com.sbase.pipeline;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class PipelineRetryWorker {
    private final PipelineRunRepository runs;
    private final PipelineExecutionService execution;

    public PipelineRetryWorker(PipelineRunRepository runs, PipelineExecutionService execution) {
        this.runs = runs;
        this.execution = execution;
    }

    @Scheduled(fixedDelayString = "${sbase.pipeline.retry.poll-ms:5000}")
    public void poll() {
        runs.findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByNextAttemptAtAsc("RETRY_WAIT", Instant.now())
            .forEach(run -> execution.retryDue(run.getId()));
    }
}
