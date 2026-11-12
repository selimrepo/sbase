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
    @Autowired CatalogColumnRepository columns;
    @Autowired LineageEdgeRepository edges;
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
        CatalogLineageService service=new CatalogLineageService(assets,columns,edges,tenantContext,new ObjectMapper());

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
        CatalogLineageService service=new CatalogLineageService(assets,columns,edges,tenantContext,new ObjectMapper());
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
}
