package com.sbase.pipeline;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="pipeline_runs",indexes=@Index(name="idx_pipeline_runs_pipeline_submitted",columnList="pipeline_id,submitted_at"))
public class PipelineRunEntity {
    private static final UUID LEGACY_TENANT=UUID.fromString("00000000-0000-0000-0000-000000000001");
    @Id private UUID id;
    @Column(name="tenant_id",nullable=false) private UUID tenantId;
    @Column(name="pipeline_id",nullable=false) private UUID pipelineId;
    @Column(name="pipeline_version",nullable=false) private int pipelineVersion;
    @Column(nullable=false,length=24) private String status;
    @Column(name="definition_snapshot",nullable=false,columnDefinition="text") private String definitionSnapshot;
    @Column(name="result_json",columnDefinition="text") private String resultJson;
    @Column(name="error_message",columnDefinition="text") private String errorMessage;
    @Column(name="submitted_at",nullable=false) private Instant submittedAt;
    @Column(name="started_at") private Instant startedAt;
    @Column(name="finished_at") private Instant finishedAt;
    @Column(name="records_processed",nullable=false) private long recordsProcessed;
    @Column(name="attempt_count",nullable=false) private int attemptCount;
    @Column(name="max_attempts",nullable=false) private int maxAttempts=4;
    @Column(name="next_attempt_at") private Instant nextAttemptAt;
    @Column(name="lease_until") private Instant leaseUntil;
    @Column(name="idempotency_key",length=160) private String idempotencyKey;
    @Column(name="submitted_by",nullable=false,length=200) private String submittedBy="system";
    @Column(name="submitted_by_subject",nullable=false,length=200) private String submittedBySubject="system";
    @Version @Column(name="version",nullable=false) private long version;

    protected PipelineRunEntity() {}
    public PipelineRunEntity(UUID pipelineId,int pipelineVersion,String definitionSnapshot) {
        this(LEGACY_TENANT,pipelineId,pipelineVersion,definitionSnapshot,"system");
    }
    public PipelineRunEntity(UUID pipelineId,int pipelineVersion,String definitionSnapshot,String submittedBy) {
        this(LEGACY_TENANT,pipelineId,pipelineVersion,definitionSnapshot,submittedBy);
    }
    public PipelineRunEntity(UUID tenantId,UUID pipelineId,int pipelineVersion,String definitionSnapshot,String submittedBy) {
        this.id=UUID.randomUUID(); this.tenantId=tenantId; this.pipelineId=pipelineId;
        this.pipelineVersion=pipelineVersion; this.definitionSnapshot=definitionSnapshot;
        this.submittedBy=submittedBy==null||submittedBy.isBlank()?"system":submittedBy;
        this.submittedBySubject=this.submittedBy; this.status="QUEUED"; this.submittedAt=Instant.now();
    }
    public void start(){status="RUNNING";startedAt=Instant.now();finishedAt=null;leaseUntil=null;attemptCount++;nextAttemptAt=null;errorMessage=null;}
    public boolean scheduleRetry(String message,Instant retryAt){
        errorMessage=message==null?"Unknown execution error":message; finishedAt=Instant.now(); leaseUntil=null;
        if(attemptCount>=maxAttempts){status="FAILED";nextAttemptAt=null;return false;}
        status="RETRY_WAIT";nextAttemptAt=retryAt;return true;
    }
    public void setMaxAttempts(int value){if(value<1||value>20)throw new IllegalArgumentException("maxAttempts must be 1..20");maxAttempts=value;}
    public void setIdempotencyKey(String value){idempotencyKey=value;}
    public int getAttemptCount(){return attemptCount;} public int getMaxAttempts(){return maxAttempts;}
    public Instant getNextAttemptAt(){return nextAttemptAt;} public Instant getLeaseUntil(){return leaseUntil;}
    public String getIdempotencyKey(){return idempotencyKey;} public String getSubmittedBy(){return submittedBy;}
    public UUID getTenantId(){return tenantId;}
    public void succeed(String resultJson,long records){status="SUCCEEDED";this.resultJson=resultJson;recordsProcessed=records;finishedAt=Instant.now();leaseUntil=null;}
    public void cancel(){status="CANCELLED";errorMessage="Cancelled by user";finishedAt=Instant.now();leaseUntil=null;nextAttemptAt=null;}
    public void fail(String message){status="FAILED";errorMessage=message==null?"Unknown execution error":message;finishedAt=Instant.now();leaseUntil=null;nextAttemptAt=null;}
    public UUID getId(){return id;} public UUID getPipelineId(){return pipelineId;} public int getPipelineVersion(){return pipelineVersion;}
    public String getStatus(){return status;} public String getDefinitionSnapshot(){return definitionSnapshot;}
    public String getResultJson(){return resultJson;} public String getErrorMessage(){return errorMessage;}
    public Instant getSubmittedAt(){return submittedAt;} public Instant getStartedAt(){return startedAt;}
    public Instant getFinishedAt(){return finishedAt;} public long getRecordsProcessed(){return recordsProcessed;}
}
