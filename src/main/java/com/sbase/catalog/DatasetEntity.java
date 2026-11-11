package com.sbase.catalog;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "datasets", uniqueConstraints = @UniqueConstraint(name = "uk_datasets_tenant_namespace_table", columnNames = {"tenant_id","namespace","table_name"}))
public class DatasetEntity {
    @Id private UUID id;
    @Column(name="tenant_id", nullable=false) private UUID tenantId;
    @Column(name="owner_subject", nullable=false,length=200) private String ownerSubject;
    @Column(nullable = false, length = 120) private String name;
    @Column(nullable = false, length = 120) private String namespace;
    @Column(name = "table_name", nullable = false) private String tableName;
    @Column(columnDefinition = "text") private String description;
    @Column(name = "source_type", nullable = false, length = 32) private String sourceType;
    @Column(name = "schema_json", nullable = false, columnDefinition = "text") private String schemaJson;
    @Column(name = "created_at", nullable = false) private Instant createdAt;
    @Column(name = "updated_at", nullable = false) private Instant updatedAt;

    protected DatasetEntity() { }
    public DatasetEntity(UUID tenantId,String ownerSubject,String name,String namespace,String tableName,String description,String sourceType,String schemaJson) {
        this.id=UUID.randomUUID(); this.tenantId=tenantId; this.ownerSubject=ownerSubject; this.name=name;
        this.namespace=namespace; this.tableName=tableName; this.description=description; this.sourceType=sourceType;
        this.schemaJson=schemaJson; this.createdAt=Instant.now(); this.updatedAt=createdAt;
    }
    public void update(String name,String description,String schemaJson) {
        this.name=name; this.description=description; this.schemaJson=schemaJson; this.updatedAt=Instant.now();
    }
    public UUID getId(){return id;} public UUID getTenantId(){return tenantId;} public String getOwnerSubject(){return ownerSubject;}
    public String getName(){return name;} public String getNamespace(){return namespace;} public String getTableName(){return tableName;}
    public String getDescription(){return description;} public String getSourceType(){return sourceType;} public String getSchemaJson(){return schemaJson;}
    public Instant getCreatedAt(){return createdAt;} public Instant getUpdatedAt(){return updatedAt;}
}
