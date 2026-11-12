package com.sbase.catalog;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="lineage_edges",uniqueConstraints=@UniqueConstraint(name="uq_lineage_fingerprint",columnNames={"tenant_id","edge_fingerprint"}))
public class LineageEdgeEntity {
    @Id private UUID id;
    @Column(name="tenant_id",nullable=false,updatable=false) private UUID tenantId;
    @Column(name="from_asset_id",nullable=false,updatable=false) private UUID fromAssetId;
    @Column(name="to_asset_id",nullable=false,updatable=false) private UUID toAssetId;
    @Column(name="edge_type",nullable=false,length=32,updatable=false) private String edgeType;
    @Column(name="evidence_type",nullable=false,length=24,updatable=false) private String evidenceType;
    @Column(name="evidence_ref",length=512) private String evidenceRef;
    @Column(name="expression_text",columnDefinition="text") private String expressionText;
    @Column(nullable=false,precision=4,scale=3) private BigDecimal confidence;
    @Column(name="observed_at",nullable=false) private Instant observedAt;
    @Column(name="effective_version",nullable=false) private long effectiveVersion;
    @Column(name="run_id") private UUID runId;
    @Column(name="edge_fingerprint",nullable=false,length=64,updatable=false) private String edgeFingerprint;
    @Column(name="created_at",nullable=false,updatable=false) private Instant createdAt;
    protected LineageEdgeEntity() {}
    public LineageEdgeEntity(UUID tenantId,UUID fromAssetId,UUID toAssetId,String edgeType,String evidenceType,
            String evidenceRef,String expressionText,BigDecimal confidence,long effectiveVersion,UUID runId,String fingerprint) {
        this.id=UUID.randomUUID(); this.tenantId=tenantId; this.fromAssetId=fromAssetId; this.toAssetId=toAssetId;
        this.edgeType=edgeType; this.evidenceType=evidenceType; this.evidenceRef=evidenceRef; this.expressionText=expressionText;
        this.confidence=confidence; this.effectiveVersion=effectiveVersion; this.runId=runId;
        this.edgeFingerprint=fingerprint; this.observedAt=Instant.now(); this.createdAt=observedAt;
    }
    public UUID getId(){return id;} public UUID getTenantId(){return tenantId;} public UUID getFromAssetId(){return fromAssetId;}
    public UUID getToAssetId(){return toAssetId;} public String getEdgeType(){return edgeType;} public String getEvidenceType(){return evidenceType;}
    public String getEvidenceRef(){return evidenceRef;} public String getExpressionText(){return expressionText;}
    public BigDecimal getConfidence(){return confidence;} public Instant getObservedAt(){return observedAt;}
    public long getEffectiveVersion(){return effectiveVersion;} public UUID getRunId(){return runId;} public String getEdgeFingerprint(){return edgeFingerprint;}
}