package com.sbase.pipeline;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "pipeline_runs", indexes = {
        @Index(name = "idx_pipeline_runs_pipeline_submitted", columnList = "pipeline_id,submitted_at"),
        @Index(name = "idx_pipeline_runs_status", columnList = "status")
})
public class PipelineRunEntity {
    @Id private UUID id;
    @Column(name = "pipeline_id", nullable = false) private UUID pipelineId;
    @Column(name = "pipeline_version", nullable = false) private int pipelineVersion;
    @Column(name = "trigger_type", nullable = false, length = 24) private String triggerType;
    @Column(name = "schedule_id") private UUID scheduleId;
    @Column(nullable = false, length = 24) private String status;
    @Column(name = "definition_snapshot", nullable = false, columnDefinition = "text") private String definitionSnapshot;
    @Column(name = "submitted_at", nullable = false) private Instant submittedAt;
    @Column(name = "started_at") private Instant startedAt;
    @Column(name = "finished_at") private Instant finishedAt;
    @Column(name = "records_read", nullable = false) private long recordsRead;
    @Column(name = "records_written", nullable = false) private long recordsWritten;
    @Column(name = "output_location", columnDefinition = "text") private String outputLocation;
    @Column(name = "step_results_json", nullable = false, columnDefinition = "text") private String stepResultsJson;
    @Column(name = "result_summary", columnDefinition = "text") private String resultSummary;
    @Column(name = "error_message", columnDefinition = "text") private String errorMessage;

    protected PipelineRunEntity() { }
    public PipelineRunEntity(UUID id, PipelineEntity pipeline, String triggerType, UUID scheduleId) {
        this.id=id; this.pipelineId=pipeline.getId(); this.pipelineVersion=pipeline.getVersion();
        this.triggerType=triggerType; this.scheduleId=scheduleId; this.status="QUEUED";
        this.definitionSnapshot=pipeline.getDefinitionJson(); this.submittedAt=Instant.now(); this.stepResultsJson="[]";
    }
    public void start() { status="RUNNING"; startedAt=Instant.now(); }
    public void succeed(long read, long written, String output, String steps, String summary) {
        status="SUCCEEDED"; recordsRead=read; recordsWritten=written; outputLocation=output;
        stepResultsJson=steps; resultSummary=summary; finishedAt=Instant.now();
    }
    public void fail(String error, String steps) {
        status="FAILED"; errorMessage=error; stepResultsJson=steps; finishedAt=Instant.now();
    }
    public UUID getId(){return id;} public UUID getPipelineId(){return pipelineId;}
    public int getPipelineVersion(){return pipelineVersion;} public String getTriggerType(){return triggerType;}
    public UUID getScheduleId(){return scheduleId;} public String getStatus(){return status;}
    public String getDefinitionSnapshot(){return definitionSnapshot;} public Instant getSubmittedAt(){return submittedAt;}
    public Instant getStartedAt(){return startedAt;} public Instant getFinishedAt(){return finishedAt;}
    public long getRecordsRead(){return recordsRead;} public long getRecordsWritten(){return recordsWritten;}
    public String getOutputLocation(){return outputLocation;} public String getStepResultsJson(){return stepResultsJson;}
    public String getResultSummary(){return resultSummary;} public String getErrorMessage(){return errorMessage;}
}
