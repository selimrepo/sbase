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
        Instant now = Instant.now();
        runs.findTop100ByStatusAndLeaseUntilBeforeOrderByLeaseUntilAsc("RUNNING", now)
            .forEach(run -> execution.recoverExpired(run.getId()));
        runs.findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByNextAttemptAtAsc("RETRY_WAIT", now)
            .forEach(run -> execution.retryDue(run.getId()));
        runs.findTop100ByStatusAndSubmittedAtLessThanEqualOrderBySubmittedAtAsc("QUEUED", now.minusSeconds(30))
            .forEach(run -> execution.dispatchQueued(run.getId()));
    }
}
