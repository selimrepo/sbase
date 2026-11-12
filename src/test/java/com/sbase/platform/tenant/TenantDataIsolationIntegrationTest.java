package com.sbase.platform.tenant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.bi.BiWorkspaceService;
import com.sbase.platform.bi.GovernedBiQueryService;
import com.sbase.platform.feature.OnlineFeatureStoreService;
import com.sbase.platform.ml.MlLifecycleService;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.server.ResponseStatusException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@DataJpaTest
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class TenantDataIsolationIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("sbase").withUsername("sbase").withPassword("sbase");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.flyway.enabled", () -> true);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
    }

    @Autowired JdbcTemplate jdbc;
    private final ObjectMapper mapper = new ObjectMapper();

    private UUID[] tenants() {
        UUID a=UUID.randomUUID(), b=UUID.randomUUID();
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",a,"iso-a-"+a,"Isolation A");
        jdbc.update("INSERT INTO tenants(id,slug,display_name,status) VALUES(?,?,?,'ACTIVE')",b,"iso-b-"+b,"Isolation B");
        return new UUID[]{a,b};
    }

    @Test
    void onlineFeatureValuesAreIsolatedEvenWhenEntityKeysAreIdentical() {
        UUID[] ids=tenants();
        OnlineFeatureStoreService service=new OnlineFeatureStoreService(jdbc,mapper);
        service.upsert(ids[0],"customer_profile_iso","customer-1",1,Instant.now(),Map.of("secret","tenant-a"));
        service.upsert(ids[1],"customer_profile_iso","customer-1",1,Instant.now(),Map.of("secret","tenant-b"));

        assertThat(service.get(ids[0],"customer_profile_iso","customer-1").orElseThrow())
            .containsEntry("secret","tenant-a");
        assertThat(service.get(ids[1],"customer_profile_iso","customer-1").orElseThrow())
            .containsEntry("secret","tenant-b");
        assertThat(service.count(ids[0],"customer_profile_iso")).isEqualTo(1);
        assertThat(service.count(ids[1],"customer_profile_iso")).isEqualTo(1);
    }

    @Test
    void reportsDashboardsWidgetsAndDashboardDataCannotCrossTenantBoundary() {
        UUID[] ids=tenants();
        BiWorkspaceService bi=new BiWorkspaceService(jdbc,mapper,mock(GovernedBiQueryService.class));
        var definition=new GovernedBiQueryService.QueryDefinition("analytics.sales",List.of("region"),
            List.of(new GovernedBiQueryService.Metric("revenue","SUM","total")),List.of(),List.of(),100);
        UUID reportA=bi.createReport(ids[0],"alice","private-report","tenant A report",definition);
        UUID dashboardA=bi.createDashboard(ids[0],"alice","private-dashboard","tenant A dashboard",Map.of(),true);
        bi.addWidget(ids[0],"alice",dashboardA,reportA,"private-widget","TABLE",Map.of(),Map.of());

        assertThat(bi.reports(ids[0],"alice")).extracting("id").contains(reportA);
        assertThat(bi.reports(ids[1],"alice")).isEmpty();
        assertThat(bi.dashboards(ids[0],"alice")).extracting("id").contains(dashboardA);
        assertThat(bi.dashboards(ids[1],"alice")).isEmpty();
        assertThrows(ResponseStatusException.class,()->bi.executeReport(ids[1],"bob",reportA));
        assertThrows(ResponseStatusException.class,()->bi.widgets(ids[1],"bob",dashboardA));
        assertThrows(ResponseStatusException.class,()->bi.dashboardData(ids[1],"bob",dashboardA));
        assertThrows(ResponseStatusException.class,()->bi.addWidget(ids[1],"bob",dashboardA,reportA,
            "foreign-widget","TABLE",Map.of(),Map.of()));
        assertThrows(ResponseStatusException.class,()->bi.shareReport(ids[1],"bob",reportA,"mallory","READ"));
    }

    @Test
    void mlExperimentsRunsModelsAndDeploymentsAreTenantScoped() {
        UUID[] ids=tenants();
        Path artifactRoot=Path.of(System.getProperty("java.io.tmpdir"),"sbase-tenant-ml-it");
        MlLifecycleService ml=new MlLifecycleService(jdbc,mapper,mock(ObjectProvider.class),Runnable::run,
            POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword(),artifactRoot.toString(),"analytics.training");

        UUID experimentA=ml.createExperiment(ids[0],"exp-a","tenant A","alice");
        UUID experimentB=ml.createExperiment(ids[1],"exp-b","tenant B","bob");
        assertThat(ml.experiments(ids[0],"alice")).extracting("id").contains(experimentA);
        assertThat(ml.experiments(ids[1],"alice")).isEmpty();

        UUID runA=UUID.randomUUID();
        jdbc.update("""
            INSERT INTO sbase_ml_runs(id,tenant_id,experiment_id,run_name,algorithm,task_type,status,
              dataset_table,feature_columns_json,label_column,submitted_by)
            VALUES(?,?,?,'run-a','LINEAR_REGRESSION','REGRESSION','SUCCEEDED',
              'analytics.training','["feature"]'::jsonb,'label','alice')
            """,runA,ids[0],experimentA);
        assertThat(ml.compare(ids[0],experimentA)).hasSize(1);
        assertThat(ml.compare(ids[1],experimentA)).isEmpty();
        assertThrows(EmptyResultDataAccessException.class,()->ml.registerModel(ids[1],runA,"foreign-model","", "bob"));

        UUID modelA=UUID.randomUUID(), versionA=UUID.randomUUID(), deploymentA=UUID.randomUUID();
        jdbc.update("INSERT INTO sbase_ml_models(id,tenant_id,name,owner_subject) VALUES(?,?,?,?)",
            modelA,ids[0],"model-a","alice");
        jdbc.update("""
            INSERT INTO sbase_ml_model_versions(id,tenant_id,model_id,run_id,version,artifact_uri,framework,
              algorithm,task_type,created_by,signature_json,feature_baseline_json)
            VALUES(?,?,?,?,1,?,'Spark MLlib','LINEAR_REGRESSION','REGRESSION','alice','{"features":["feature"],"label":"label"}'::jsonb,'{}'::jsonb)
            """,versionA,ids[0],modelA,runA,artifactRoot.resolve(ids[0].toString()).resolve("model-a").toUri().toString());
        jdbc.update("""
            INSERT INTO sbase_ml_deployments(id,tenant_id,model_version_id,deployment_name,status,created_by)
            VALUES(?,?,?,'deployment-a','ACTIVE','alice')
            """,deploymentA,ids[0],versionA);

        assertThat(ml.models(ids[0])).hasSize(1);
        assertThat(ml.models(ids[1])).isEmpty();
        assertThrows(IllegalArgumentException.class,()->ml.deploy(ids[1],versionA,"foreign-deployment","bob"));
        assertThrows(EmptyResultDataAccessException.class,()->ml.lineage(ids[1],versionA));
        assertThrows(EmptyResultDataAccessException.class,()->ml.predict(ids[1],deploymentA,Map.of("feature",1)));
    }

    @Test
    void modelArtifactUriCannotEscapeItsTenantDirectory() {
        UUID[] ids=tenants();
        Path artifactRoot=Path.of(System.getProperty("java.io.tmpdir"),"sbase-tenant-artifact-guard");
        MlLifecycleService ml=new MlLifecycleService(jdbc,mapper,mock(ObjectProvider.class),Runnable::run,
            POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword(),artifactRoot.toString(),"analytics.training");
        UUID experiment=ml.createExperiment(ids[0],"artifact-guard","test","alice");
        UUID run=UUID.randomUUID(),model=UUID.randomUUID(),version=UUID.randomUUID(),deployment=UUID.randomUUID();
        jdbc.update("""
            INSERT INTO sbase_ml_runs(id,tenant_id,experiment_id,run_name,algorithm,task_type,status,
              dataset_table,feature_columns_json,label_column,submitted_by)
            VALUES(?,?,?,'run','LINEAR_REGRESSION','REGRESSION','SUCCEEDED','analytics.training',
              '["feature"]'::jsonb,'label','alice')
            """,run,ids[0],experiment);
        jdbc.update("INSERT INTO sbase_ml_models(id,tenant_id,name,owner_subject) VALUES(?,?,?,?)",model,ids[0],"artifact-model","alice");
        String foreignArtifact=artifactRoot.resolve(ids[1].toString()).resolve("foreign-model").toUri().toString();
        jdbc.update("""
            INSERT INTO sbase_ml_model_versions(id,tenant_id,model_id,run_id,version,artifact_uri,framework,
              algorithm,task_type,created_by,signature_json,feature_baseline_json)
            VALUES(?,?,?,?,1,?,'Spark MLlib','LINEAR_REGRESSION','REGRESSION','alice','{}'::jsonb,'{}'::jsonb)
            """,version,ids[0],model,run,foreignArtifact);
        jdbc.update("""
            INSERT INTO sbase_ml_deployments(id,tenant_id,model_version_id,deployment_name,status,created_by)
            VALUES(?,?,?,'artifact-guard','ACTIVE','alice')
            """,deployment,ids[0],version);

        var failure=org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
            ()->ml.predict(ids[0],deployment,Map.of("feature",1)));
        assertThat(failure.getCause()).isInstanceOf(AccessDeniedException.class);
    }
}
