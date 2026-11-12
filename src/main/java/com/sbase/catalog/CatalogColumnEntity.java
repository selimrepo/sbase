package com.sbase.catalog;

import jakarta.persistence.*;
import java.util.UUID;

@Entity
@Table(name="catalog_columns",uniqueConstraints=@UniqueConstraint(name="uq_catalog_column_ordinal",columnNames={"tenant_id","table_asset_id","ordinal_position"}))
public class CatalogColumnEntity {
    @Id @Column(name="id") private UUID id;
    @Column(name="tenant_id",nullable=false,updatable=false) private UUID tenantId;
    @Column(name="table_asset_id",nullable=false,updatable=false) private UUID tableAssetId;
    @Column(name="ordinal_position",nullable=false) private int ordinalPosition;
    @Column(name="data_type",nullable=false,length=160) private String dataType;
    @Column(nullable=false) private boolean nullable;
    @Column(name="default_expression",columnDefinition="text") private String defaultExpression;
    @Column(name="generated_expression",columnDefinition="text") private String generatedExpression;
    protected CatalogColumnEntity() {}
    public CatalogColumnEntity(UUID id,UUID tenantId,UUID tableAssetId,int ordinalPosition,String dataType,boolean nullable) {
        this.id=id; this.tenantId=tenantId; this.tableAssetId=tableAssetId; this.ordinalPosition=ordinalPosition;
        this.dataType=dataType; this.nullable=nullable;
    }
    public void updateSchema(int ordinalPosition,String dataType,boolean nullable) {
        this.ordinalPosition=ordinalPosition; this.dataType=dataType; this.nullable=nullable;
    }
    public UUID getId(){return id;} public UUID getTenantId(){return tenantId;} public UUID getTableAssetId(){return tableAssetId;}
    public int getOrdinalPosition(){return ordinalPosition;} public String getDataType(){return dataType;} public boolean isNullable(){return nullable;}
    public String getDefaultExpression(){return defaultExpression;} public String getGeneratedExpression(){return generatedExpression;}
}