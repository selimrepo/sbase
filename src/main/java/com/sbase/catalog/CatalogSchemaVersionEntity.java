package com.sbase.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="catalog_schema_versions",uniqueConstraints=@UniqueConstraint(name="uq_catalog_schema_version",columnNames={"tenant_id","table_asset_id","schema_fingerprint"}))
public class CatalogSchemaVersionEntity {
    @Id private UUID id;
    @Column(name="tenant_id",nullable=false,updatable=false) private UUID tenantId;
    @Column(name="table_asset_id",nullable=false,updatable=false) private UUID tableAssetId;
    @Column(name="schema_fingerprint",nullable=false,length=64,updatable=false) private String schemaFingerprint;
    @JdbcTypeCode(SqlTypes.JSON) @Column(name="schema_json",nullable=false,columnDefinition="jsonb",updatable=false) private JsonNode schemaJson;
    @Column(name="source_ref",length=512,updatable=false) private String sourceRef;
    @Column(name="observed_at",nullable=false,updatable=false) private Instant observedAt;
    @Column(name="created_by",nullable=false,length=200,updatable=false) private String createdBy;
    protected CatalogSchemaVersionEntity() {}
    public CatalogSchemaVersionEntity(UUID tenantId,UUID tableAssetId,String fingerprint,JsonNode schema,String sourceRef,String createdBy) {
        this.id=UUID.randomUUID();this.tenantId=tenantId;this.tableAssetId=tableAssetId;this.schemaFingerprint=fingerprint;
        this.schemaJson=schema;this.sourceRef=sourceRef;this.createdBy=createdBy;this.observedAt=Instant.now();
    }
    public UUID getId(){return id;} public UUID getTenantId(){return tenantId;} public UUID getTableAssetId(){return tableAssetId;}
    public String getSchemaFingerprint(){return schemaFingerprint;} public JsonNode getSchemaJson(){return schemaJson;}
    public String getSourceRef(){return sourceRef;} public Instant getObservedAt(){return observedAt;} public String getCreatedBy(){return createdBy;}
}