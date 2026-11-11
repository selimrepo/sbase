package com.sbase.mining;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="mining_jobs",indexes={
    @Index(name="idx_mining_jobs_submitted_at",columnList="submitted_at"),
    @Index(name="idx_mining_jobs_status",columnList="status"),
    @Index(name="idx_mining_jobs_tenant_submitted",columnList="tenant_id,submitted_at")
})
public class MiningJobEntity {
    private static final UUID LEGACY_TENANT=UUID.fromString("00000000-0000-0000-0000-000000000001");
    @Id private UUID id;
    @Column(name="tenant_id",nullable=false) private UUID tenantId;
    @Column(name="owner_subject",nullable=false,length=200) private String ownerSubject;
    @Column(nullable=false,length=40) private String operation;
    @Column(name="source_table",nullable=false) private String sourceTable;
    @Column(name="output_table",nullable=false) private String outputTable;
    @Enumerated(EnumType.STRING) @Column(nullable=false,length=24) private MiningJobStatus status;
    @Column(name="submitted_at",nullable=false) private Instant submittedAt;
    @Column(name="started_at") private Instant startedAt;
    @Column(name="finished_at") private Instant finishedAt;
    @Column(name="result_summary",columnDefinition="text") private String resultSummary;
    @Column(name="error_message",columnDefinition="text") private String errorMessage;
    protected MiningJobEntity() {}
    public MiningJobEntity(UUID id,String operation,String sourceTable,String outputTable) {
        this(id,LEGACY_TENANT,"system",operation,sourceTable,outputTable);
    }
    public MiningJobEntity(UUID id,UUID tenantId,String ownerSubject,String operation,String sourceTable,String outputTable) {
        this.id=id; this.tenantId=tenantId; this.ownerSubject=ownerSubject; this.operation=operation;
        this.sourceTable=sourceTable; this.outputTable=outputTable; this.status=MiningJobStatus.QUEUED; this.submittedAt=Instant.now();
    }
    public void start(){status=MiningJobStatus.RUNNING;startedAt=Instant.now();}
    public void succeed(String summary){status=MiningJobStatus.SUCCEEDED;resultSummary=summary;finishedAt=Instant.now();}
    public void fail(String error){status=MiningJobStatus.FAILED;errorMessage=error;finishedAt=Instant.now();}
    public UUID getId(){return id;} public UUID getTenantId(){return tenantId;} public String getOwnerSubject(){return ownerSubject;}
    public String getOperation(){return operation;} public String getSourceTable(){return sourceTable;} public String getOutputTable(){return outputTable;}
    public MiningJobStatus getStatus(){return status;} public Instant getSubmittedAt(){return submittedAt;}
    public Instant getStartedAt(){return startedAt;} public Instant getFinishedAt(){return finishedAt;}
    public String getResultSummary(){return resultSummary;} public String getErrorMessage(){return errorMessage;}
}
