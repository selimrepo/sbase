package com.sbase.pipeline;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "pipeline_runs", indexes = {
        @Index(name = "idx_pipeline_runs_pipeline_submitted", columnList = "pipeline_id,submitted_at")
})
public class PipelineRunEntity {
    @Id private UUID id;
    @Column(name = "pipeline_id", nullable = false) private UUID pipelineId;
    @Column(name = "pipeline_version", nullable = false) private int pipelineVersion;
    @Column(nullable = false, length = 24) private String status;
    @Column(name = "definition_snapshot", nullable = false, columnDefinition = "text") private String definitionSnapshot;
    @Column(name = "result_json", columnDefinition = "text") private String resultJson;
    @Column(name = "error_message", columnDefinition = "text") private String errorMessage;
    @Column(name = "submitted_at", nullable = false) private Instant submittedAt;
    @Column(name = "started_at") private Instant startedAt;
    @Column(name = "finished_at") private Instant finishedAt;
    @Column(name = "records_processed", nullable = false) private long recordsProcessed;

    protected PipelineRunEntity() { }
    public PipelineRunEntity(UUID pipelineId, int pipelineVersion, String definitionSnapshot) {
        this.id = UUID.randomUUID();
        this.pipelineId = pipelineId;
        this.pipelineVersion = pipelineVersion;
        this.definitionSnapshot = definitionSnapshot;
        this.status = "QUEUED";
        this.submittedAt = Instant.now();
    }
    public void start() { status = "RUNNING"; startedAt = Instant.now(); errorMessage = null; }
    public void succeed(String resultJson, long records) {
        status = "SUCCEEDED"; this.resultJson = resultJson; recordsProcessed = records; finishedAt = Instant.now();
    }
    public void fail(String message) {
        status = "FAILED"; errorMessage = message == null ? "Unknown execution error" : message;
        finishedAt = Instant.now();
    }
    public UUID getId() { return id; }
    public UUID getPipelineId() { return pipelineId; }
    public int getPipelineVersion() { return pipelineVersion; }
    public String getStatus() { return status; }
    public String getDefinitionSnapshot() { return definitionSnapshot; }
    public String getResultJson() { return resultJson; }
    public String getErrorMessage() { return errorMessage; }
    public Instant getSubmittedAt() { return submittedAt; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
    public long getRecordsProcessed() { return recordsProcessed; }
}
