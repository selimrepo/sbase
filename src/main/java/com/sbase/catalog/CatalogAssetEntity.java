package com.sbase.catalog;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="catalog_assets", uniqueConstraints=@UniqueConstraint(name="uq_catalog_asset_external",columnNames={"tenant_id","asset_type","external_key"}))
public class CatalogAssetEntity {
    @Id private UUID id;
    @Column(name="tenant_id",nullable=false,updatable=false) private UUID tenantId;
    @Column(name="asset_type",nullable=false,length=32,updatable=false) private String assetType;
    @Column(name="external_key",nullable=false,length=512,updatable=false) private String externalKey;
    @Column(name="display_name",nullable=false,length=240) private String displayName;
    @Column(length=160) private String namespace;
    @Column(name="physical_name",length=512) private String physicalName;
    @Column(columnDefinition="text") private String description;
    @Column(name="owner_subject",nullable=false,length=200) private String ownerSubject;
    @Column(name="owner_team",length=160) private String ownerTeam;
    @Column(length=120) private String domain;
    @Column(nullable=false,length=24) private String classification="INTERNAL";
    @JdbcTypeCode(SqlTypes.JSON) @Column(name="tags_json",nullable=false,columnDefinition="jsonb") private java.util.List<String> tags=java.util.List.of();
    @JdbcTypeCode(SqlTypes.JSON) @Column(name="metadata_json",nullable=false,columnDefinition="jsonb") private java.util.Map<String,Object> metadata=java.util.Map.of();
    @Column(name="parent_id") private UUID parentId;
    @Column(name="lifecycle_state",nullable=false,length=24) private String lifecycleState="ACTIVE";
    @Column(name="schema_fingerprint",length=64) private String schemaFingerprint;
    @Column(name="created_at",nullable=false,updatable=false) private Instant createdAt;
    @Column(name="updated_at",nullable=false) private Instant updatedAt;
    @Version @Column(name="row_version",nullable=false) private long rowVersion;

    protected CatalogAssetEntity() {}
    public CatalogAssetEntity(UUID tenantId,String assetType,String externalKey,String displayName,String ownerSubject) {
        this.id=UUID.randomUUID(); this.tenantId=tenantId; this.assetType=assetType; this.externalKey=externalKey;
        this.displayName=displayName; this.ownerSubject=ownerSubject; this.createdAt=Instant.now(); this.updatedAt=createdAt;
    }
    public void updateGovernance(String description,String ownerTeam,String domain,String classification,java.util.List<String> tags,java.util.Map<String,Object> metadata) {
        this.description=description; this.ownerTeam=ownerTeam; this.domain=domain; this.classification=classification;
        this.tags=tags; this.metadata=metadata; this.updatedAt=Instant.now();
    }
    public void setPhysicalLocation(String namespace,String physicalName) { this.namespace=namespace; this.physicalName=physicalName; this.updatedAt=Instant.now(); }
    public void updateSourceDetails(String displayName,String description,String namespace,String physicalName) {
        this.displayName=displayName; this.description=description; this.namespace=namespace; this.physicalName=physicalName; this.updatedAt=Instant.now();
    }
    public void setParentId(UUID parentId) { this.parentId=parentId; this.updatedAt=Instant.now(); }
    public void setOwnerSubject(String ownerSubject) { this.ownerSubject=ownerSubject; this.updatedAt=Instant.now(); }
    public void setLifecycleState(String lifecycleState) { this.lifecycleState=lifecycleState; this.updatedAt=Instant.now(); }
    public void setSchemaFingerprint(String fingerprint) { this.schemaFingerprint=fingerprint; this.updatedAt=Instant.now(); }
    public UUID getId(){return id;} public UUID getTenantId(){return tenantId;} public String getAssetType(){return assetType;}
    public String getExternalKey(){return externalKey;} public String getDisplayName(){return displayName;}
    public String getNamespace(){return namespace;} public String getPhysicalName(){return physicalName;}
    public String getDescription(){return description;} public String getOwnerSubject(){return ownerSubject;}
    public String getOwnerTeam(){return ownerTeam;} public String getDomain(){return domain;}
    public String getClassification(){return classification;} public String getTagsJson(){return tagsJson;}
    public java.util.Map<String,Object> getMetadata(){return metadata;} public java.util.List<String> getTags(){return tags;} public UUID getParentId(){return parentId;}
    public String getLifecycleState(){return lifecycleState;} public String getSchemaFingerprint(){return schemaFingerprint;}
    public Instant getCreatedAt(){return createdAt;} public Instant getUpdatedAt(){return updatedAt;} public long getRowVersion(){return rowVersion;}
}