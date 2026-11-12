package com.sbase.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.tenant.TenantContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@DataJpaTest
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class CatalogLineageIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("sbase_catalog").withUsername("sbase").withPassword("sbase");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",POSTGRES::getUsername);
        registry.add("spring.datasource.password",POSTGRES::getPassword);
        registry.add("spring.flyway.enabled",()->true);
        registry.add("spring.jpa.hibernate.ddl-auto",()->"validate");
    }

    @Autowired CatalogAssetRepository assets;
    @Autowired DatasetRepository datasets;
    @Autowired com.sbase.pipeline.PipelineRepository pipelines;
    @Autowired CatalogColumnRepository columns;
    @Autowired LineageEdgeRepository edges;
    @Autowired CatalogSchemaVersionRepository schemaVersions;
    @Autowired JdbcTemplate jdbc;

    @Test
    void persistsGovernedCatalogColumnsAndTraversableLineageWithinTenant() {
        UUID tenant=UUID.randomUUID(), otherTenant=UUID.randomUUID();
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",tenant,"catalog-a-"+tenant,"Catalog A");
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",otherTenant,"catalog-b-"+otherTenant,"Catalog B");
        TenantContext tenantContext=mock(TenantContext.class);
        var auth=new TestingAuthenticationToken("catalog-owner","n/a");
        when(tenantContext.tenantId(auth)).thenReturn(tenant);
        when(tenantContext.subject(auth)).thenReturn("catalog-owner");
        CatalogLineageService service=new CatalogLineageService(assets,columns,edges,tenantContext,new ObjectMapper(),schemaVersions);

        var table=service.createAsset(new CatalogLineageService.CreateAsset("TABLE","warehouse.orders","Orders",
            "warehouse","orders","Curated order facts","Data Engineering","Sales","CONFIDENTIAL",
            List.of("sales","gold"),Map.of("system","warehouse"),null),auth);
        var column=service.registerColumn(table.id(),new CatalogLineageService.CreateColumn("customer_id","UUID",false,
            1,"Customer key","Data Engineering","RESTRICTED",List.of("pii"),Map.of("semanticType","identifier")),auth);
        var pipeline=service.createAsset(new CatalogLineageService.CreateAsset("PIPELINE","pipeline:orders-enrichment",
            "Orders enrichment",null,null,null,"Data Engineering","Sales","INTERNAL",List.of(),Map.of(),null),auth);
        var edge=service.connect(new CatalogLineageService.CreateEdge(column.id(),pipeline.id(),"READS_FROM",
            "USER_DECLARED","pipeline:orders-enrichment","customer_id join key",new BigDecimal("0.950"),1L,null),auth);

        assertThat(column.assetType()).isEqualTo("COLUMN");
        assertThat(column.dataType()).isEqualTo("UUID");
        assertThat(column.nullable()).isFalse();
        assertThat(service.columns(table.id(),auth)).extracting(CatalogLineageService.AssetView::displayName)
            .containsExactly("customer_id");

        var impact=service.impact(column.id(),4,auth);
        assertThat(impact.nodes()).extracting(CatalogLineageService.AssetView::id).contains(pipeline.id());
        assertThat(impact.edges()).extracting(CatalogLineageService.EdgeView::id).contains(edge.id());

        var duplicate=service.connect(new CatalogLineageService.CreateEdge(column.id(),pipeline.id(),"READS_FROM",
            "USER_DECLARED","pipeline:orders-enrichment","customer_id join key",new BigDecimal("0.950"),1L,null),auth);
        assertThat(duplicate.id()).isEqualTo(edge.id());
        assertThat(edges.findAllByTenantIdAndFromAssetIdIn(tenant,List.of(column.id()))).hasSize(1);

        var otherAuth=new TestingAuthenticationToken("other-user","n/a");
        when(tenantContext.tenantId(otherAuth)).thenReturn(otherTenant);
        assertThatThrownBy(()->service.get(table.id(),otherAuth))
            .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        assertThat(assets.findByIdAndTenantId(table.id(),otherTenant)).isEmpty();
        assertThat(edges.findAllByTenantIdAndFromAssetIdIn(otherTenant,List.of(column.id()))).isEmpty();
    }

    @Test
    void preventsDuplicateColumnsAndRejectsCrossTenantEdges() {
        UUID tenant=UUID.randomUUID(), other=UUID.randomUUID();
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",tenant,"catalog-c-"+tenant,"Catalog C");
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",other,"catalog-d-"+other,"Catalog D");
        TenantContext tenantContext=mock(TenantContext.class);
        var auth=new TestingAuthenticationToken("owner","n/a");
        when(tenantContext.tenantId(auth)).thenReturn(tenant);
        when(tenantContext.subject(auth)).thenReturn("owner");
        CatalogLineageService service=new CatalogLineageService(assets,columns,edges,tenantContext,new ObjectMapper(),schemaVersions);
        var table=service.createAsset(new CatalogLineageService.CreateAsset("TABLE","crm.contacts","Contacts",
            "crm","contacts",null,null,null,null,List.of(),Map.of(),null),auth);
        var col=service.registerColumn(table.id(),new CatalogLineageService.CreateColumn("email","VARCHAR",true,1,
            null,null,null,List.of(),Map.of()),auth);
        assertThatThrownBy(()->service.registerColumn(table.id(),new CatalogLineageService.CreateColumn("email","VARCHAR",true,2,
            null,null,null,List.of(),Map.of()),auth))
            .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);

        var foreign=new CatalogAssetEntity(other,"TABLE","foreign.table","Foreign table","other");
        assets.saveAndFlush(foreign);
        assertThatThrownBy(()->service.connect(new CatalogLineageService.CreateEdge(col.id(),foreign.getId(),
            "DERIVES_FROM","USER_DECLARED",null,null,BigDecimal.ONE,1L,null),auth))
            .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
    }
    @Test
    void datasetCrudAutomaticallyMaintainsCatalogAndColumnNodes() {
        UUID tenant=UUID.randomUUID();
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",tenant,"catalog-sync-"+tenant,"Catalog Sync");
        TenantContext tenantContext=mock(TenantContext.class);
        var auth=new TestingAuthenticationToken("dataset-owner","n/a");
        when(tenantContext.tenantId(auth)).thenReturn(tenant);
        when(tenantContext.subject(auth)).thenReturn("dataset-owner");
        CatalogLineageService lineage=new CatalogLineageService(assets,columns,edges,tenantContext,new ObjectMapper(),schemaVersions);
        DatasetService service=new DatasetService(datasets,tenantContext,mock(com.sbase.platform.tenant.ResourceAclService.class),lineage);

        DatasetEntity dataset=service.create(new DatasetRequest("Orders","warehouse","orders","Order facts","POSTGRESQL",
            "[{\"name\":\"order_id\",\"type\":\"UUID\",\"nullable\":false},{\"name\":\"amount\",\"dataType\":\"DECIMAL(18,2)\"}]"),auth);
        CatalogAssetEntity table=assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,"TABLE",dataset.getId().toString()).orElseThrow();
        assertThat(table.getDisplayName()).isEqualTo("Orders");
        assertThat(table.getSchemaFingerprint()).isNotBlank();
        assertThat(lineage.columns(table.getId(),auth)).extracting(CatalogLineageService.AssetView::displayName)
            .containsExactly("order_id","amount");

        DatasetEntity updated=service.update(dataset.getId(),new DatasetRequest("Orders curated","warehouse","orders",
            "Curated order facts","POSTGRESQL","[{\"name\":\"order_id\",\"type\":\"UUID\"}]"),auth);
        assertThat(updated.getName()).isEqualTo("Orders curated");
        assertThat(lineage.schemaHistory(table.getId(),auth)).hasSize(2);
        assertThat(lineage.columns(table.getId(),auth)).filteredOn(col->col.displayName().equals("amount"))
            .singleElement().extracting(CatalogLineageService.AssetView::lifecycleState).isEqualTo("DEPRECATED");
    }

    @Test
    void tracesSourceColumnThroughPipelineReportWidgetToDashboard() {
        UUID tenant=UUID.randomUUID();
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",tenant,"lineage-e2e-"+tenant,"Lineage E2E");
        TenantContext tenantContext=mock(TenantContext.class);
        var auth=new TestingAuthenticationToken("lineage-owner","n/a");
        when(tenantContext.tenantId(auth)).thenReturn(tenant);
        when(tenantContext.subject(auth)).thenReturn("lineage-owner");
        CatalogLineageService lineage=new CatalogLineageService(assets,columns,edges,tenantContext,new ObjectMapper());
        DatasetService datasetService=new DatasetService(datasets,tenantContext,mock(com.sbase.platform.tenant.ResourceAclService.class),lineage);
        DatasetEntity source=datasetService.create(new DatasetRequest("Orders","warehouse","orders",null,"POSTGRESQL",
            "[{\"name\":\"order_id\",\"type\":\"UUID\"}]"),auth);
        DatasetEntity output=datasetService.create(new DatasetRequest("Orders archive","warehouse","orders_archive",null,"DORIS",
            "[{\"name\":\"order_id\",\"type\":\"UUID\"}]"),auth);

        String definition="{\"steps\":["
            +"{\"id\":\"source\",\"type\":\"doris_read\",\"table\":\"warehouse.orders\"},"
            +"{\"id\":\"projection\",\"type\":\"select\",\"dependsOn\":[\"source\"],\"columns\":[\"order_id\"]},"
            +"{\"id\":\"publish\",\"type\":\"doris_write\",\"dependsOn\":[\"projection\"],"
            +"\"table\":\"warehouse.orders_archive\",\"idempotencyColumns\":[\"order_id\"]}]}";
        var pipelineService=new com.sbase.pipeline.PipelineService(pipelines,
            new com.sbase.pipeline.PipelineDefinitionValidator(new ObjectMapper()),tenantContext,
            mock(com.sbase.platform.tenant.ResourceAclService.class),lineage);
        var pipeline=pipelineService.create(new com.sbase.pipeline.PipelineRequest("Orders ETL","Lineage integration",definition,true),auth);

        UUID reportId=UUID.randomUUID(),dashboardId=UUID.randomUUID(),widgetId=UUID.randomUUID();
        lineage.synchronizeReport(reportId,tenant,"Order totals",null,"lineage-owner",
            "{\"table\":\"warehouse.orders_archive\",\"dimensions\":[\"order_id\"],\"metrics\":[{\"column\":\"order_id\",\"aggregate\":\"count\"}]}");
        lineage.synchronizeDashboard(dashboardId,tenant,"Sales dashboard",null,"lineage-owner");
        lineage.synchronizeWidget(widgetId,tenant,dashboardId,reportId,"Order totals","KPI","lineage-owner");

        CatalogAssetEntity sourceTable=assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,"TABLE",source.getId().toString()).orElseThrow();
        CatalogAssetEntity sourceColumn=assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,"COLUMN",source.getId()+":order_id").orElseThrow();
        var impact=lineage.impact(sourceColumn.getId(),8,auth);
        assertThat(impact.nodes()).extracting(CatalogLineageService.AssetView::assetType)
            .contains("PIPELINE_STEP","TABLE","REPORT","WIDGET","DASHBOARD");
        assertThat(impact.nodes()).extracting(CatalogLineageService.AssetView::id).contains(
            assets.findByTenantIdAndAssetTypeAndExternalKey(tenant,"DASHBOARD",dashboardId.toString()).orElseThrow().getId());
        assertThat(impact.edges()).extracting(CatalogLineageService.EdgeView::edgeType)
            .contains("USES_COLUMN","DEPENDS_ON","WRITES_TO","READS_FROM","DISPLAYS","PUBLISHES");
        assertThat(sourceTable.getTenantId()).isEqualTo(tenant);
        assertThat(pipeline.getTenantId()).isEqualTo(tenant);
    }

}
