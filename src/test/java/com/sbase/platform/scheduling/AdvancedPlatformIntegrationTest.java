package com.sbase.platform.scheduling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.pipeline.PipelineEntity;
import com.sbase.pipeline.PipelineExecutionService;
import com.sbase.pipeline.PipelineRepository;
import com.sbase.platform.AdvancedWorkspaceController;
import com.sbase.platform.DashboardLayoutController;
import com.sbase.platform.bi.BiReportScheduleWorker;
import com.sbase.platform.bi.DorisBiQueryService;
import com.sbase.platform.feature.FeatureAccessService;
import com.sbase.platform.feature.FeatureEngineeringController;
import com.sbase.platform.feature.SparkFeatureEngineeringService;
import com.sbase.platform.security.PermissionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DataJpaTest
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class AdvancedPlatformIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("sbase_advanced")
        .withUsername("sbase")
        .withPassword("sbase");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.flyway.enabled", () -> true);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
    }

    @Autowired JdbcTemplate db;
    @Autowired PipelineRepository pipelines;

    private PermissionService permissions() { return new PermissionService(db); }
    private TestingAuthenticationToken auth(String subject, String... authorities) {
        return new TestingAuthenticationToken(subject, "n/a",
            java.util.Arrays.stream(authorities).map(SimpleGrantedAuthority::new).toList());
    }

    @Test
    void schedulerClaimsDependenciesOnlyAfterParentSuccess() {
        PipelineEntity pipeline = pipelines.saveAndFlush(new PipelineEntity(
            "scheduler integration", "dependency claim test",
            "{\"steps\":[{\"id\":\"source\",\"type\":\"doris_read\",\"table\":\"sales\"}]}", true));
        UUID parent = UUID.randomUUID(), child = UUID.randomUUID();
        insertSchedule(parent, "parent", pipeline.getId(), null);
        insertSchedule(child, "child", pipeline.getId(), parent);

        DistributedScheduleWorker worker = new DistributedScheduleWorker(
            db, Runnable::run, mock(PipelineExecutionService.class), new ObjectMapper());

        assertThat(worker.claim()).containsExactly(parent);
        db.update("UPDATE scheduled_jobs SET last_success_at=now() WHERE id=?", parent);
        assertThat(worker.claim()).containsExactly(child);
    }

    private void insertSchedule(UUID id, String name, UUID pipelineId, UUID dependsOn) {
        db.update("""
            INSERT INTO scheduled_jobs(id,name,pipeline_id,cron_expression,enabled,next_run_at,created_at,
                                       created_by,depends_on)
            VALUES(?,?,?,?,TRUE,now()-interval '1 minute',now(),'scheduler-owner',?)
            """, id, name, pipelineId, "0 * * * * *", dependsOn);
    }

    @Test
    void featureAclHonorsOwnerAndExplicitWriteGrant() {
        UUID id = UUID.randomUUID();
        db.update("""
            INSERT INTO feature_sets(id,name,description,entity_key,feature_schema_json,transformation_json,
                                     version,created_at,updated_at,lifecycle_state,owner_subject)
            VALUES(?,?,?, ?, '[]','{}',1,now(),now(),'ACTIVE',?)
            """, id, "customer_features_it", "ACL integration", "customer_id", "feature-owner");

        FeatureAccessService access = new FeatureAccessService(db, permissions());
        var owner = auth("feature-owner", "PERM_features:read", "PERM_features:write");
        var analyst = auth("feature-analyst", "PERM_features:read", "PERM_features:write");
        access.require(owner, "customer_features_it", true);
        assertThrows(org.springframework.security.access.AccessDeniedException.class,
            () -> access.require(analyst, "customer_features_it", true));

        db.update("""
            INSERT INTO feature_access_grants(feature_set_id,subject,permission,granted_by)
            VALUES(?,?,'WRITE',?)
            """, id, "feature-analyst", "feature-owner");
        access.require(analyst, "customer_features_it", true);

        SparkFeatureEngineeringService spark = mock(SparkFeatureEngineeringService.class);
        FeatureEngineeringController materializer = new FeatureEngineeringController(
            spark, access, db, new ObjectMapper());
        var request = new FeatureEngineeringController.MaterializeRequest(
            "customer_features_it", "s3://input/customer", "s3://output/customer",
            List.of(new FeatureEngineeringController.Operation("bucketize", "score", "score_bucket", 10)));
        Map<String,Object> materialized = materializer.materialize(request, owner);
        assertThat(materialized.get("status")).isEqualTo("SUCCEEDED");
        assertThat(db.queryForObject("SELECT status FROM feature_materializations WHERE feature_set_id=?", String.class, id))
            .isEqualTo("SUCCEEDED");
        assertThat(db.queryForObject("SELECT count(*) FROM feature_lineage WHERE feature_set_id=?", Integer.class, id))
            .isEqualTo(1);
    }

    @Test
    void dashboardLayoutPersistsNonOverlappingGridPositions() {
        UUID dashboard = UUID.randomUUID(), first = UUID.randomUUID(), second = UUID.randomUUID();
        db.update("INSERT INTO dashboards(id,name,description,layout_json,owner_subject,created_at) VALUES(?,?,?,?,?,now())",
            dashboard, "Operations IT", "layout test", "{}", "dashboard-owner");
        insertWidget(first, dashboard, "Revenue");
        insertWidget(second, dashboard, "Orders");

        DashboardLayoutController controller = new DashboardLayoutController(db, permissions());
        var request = new DashboardLayoutController.LayoutRequest(List.of(
            new DashboardLayoutController.WidgetPosition(first, 0, 0, 6, 4),
            new DashboardLayoutController.WidgetPosition(second, 6, 0, 6, 4)));
        controller.saveLayout(dashboard, request, auth("dashboard-owner", "PERM_bi:write"));

        assertThat(db.queryForObject("SELECT position_x FROM dashboard_widgets WHERE id=?", Integer.class, first)).isZero();
        assertThat(db.queryForObject("SELECT position_x FROM dashboard_widgets WHERE id=?", Integer.class, second)).isEqualTo(6);
        assertThat(db.queryForObject("SELECT version FROM dashboards WHERE id=?", Long.class, dashboard)).isEqualTo(2L);
        assertThrows(IllegalArgumentException.class, () -> controller.saveLayout(dashboard,
            new DashboardLayoutController.LayoutRequest(List.of(
                new DashboardLayoutController.WidgetPosition(first, 0, 0, 8, 4),
                new DashboardLayoutController.WidgetPosition(second, 6, 0, 6, 4))),
            auth("dashboard-owner", "PERM_bi:write")));
    }

    private void insertWidget(UUID id, UUID dashboard, String title) {
        db.update("""
            INSERT INTO dashboard_widgets(id,dashboard_id,title,widget_type,query_json,position_x,position_y,width,height)
            VALUES(?,?,?,?,?,0,0,6,4)
            """, id, dashboard, title, "BAR", "{\"table\":\"sales\"}");
    }

    @Test
    void sharedBiReportsArePaginatedAndExposeQueryCapability() {
        UUID report = UUID.randomUUID();
        db.update("""
            INSERT INTO bi_reports(id,name,description,owner_subject,definition_json)
            VALUES(?,?,?,?,?)
            """, report, "Revenue report IT", "share test", "report-owner",
            "{\"table\":\"sales\",\"dimensions\":[\"region\"],\"metrics\":[{\"column\":\"amount\",\"function\":\"SUM\",\"alias\":\"revenue\"}],\"filters\":[],\"limit\":1000}");

        DorisBiQueryService doris = mock(DorisBiQueryService.class);
        AdvancedWorkspaceController controller = new AdvancedWorkspaceController(db, new ObjectMapper(), doris, permissions());
        controller.shareReport(report, new AdvancedWorkspaceController.ShareRequest("report-analyst", "QUERY"),
            auth("report-owner", "PERM_bi:write"));

        Map<String,Object> page = controller.reports(0, 10, "Revenue", auth("report-analyst", "PERM_bi:read"));
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> content = (List<Map<String,Object>>) page.get("content");
        assertThat(content).hasSize(1);
        assertThat(content.get(0).get("can_query")).isEqualTo(true);
        assertThat(content.get(0).get("can_manage")).isEqualTo(false);
        assertThat(page.get("totalElements")).isEqualTo(1);
    }

    @Test
    void scheduledBiReportExecutesAndPersistsRunResult() throws Exception {
        UUID report = UUID.randomUUID(), schedule = UUID.randomUUID();
        String definition = "{\"table\":\"sales\",\"dimensions\":[\"region\"],\"metrics\":[{\"column\":\"amount\",\"function\":\"SUM\",\"alias\":\"revenue\"}],\"filters\":[],\"limit\":100}";
        db.update("INSERT INTO bi_reports(id,name,owner_subject,definition_json) VALUES(?,?,?,?)",
            report, "Scheduled report IT", "report-owner", definition);
        DorisBiQueryService doris = mock(DorisBiQueryService.class);
        when(doris.query(anyString())).thenReturn(List.of(Map.of("region", "west", "revenue", 42L)));
        BiReportScheduleWorker worker = new BiReportScheduleWorker(db, doris, new ObjectMapper(), Runnable::run);
        Field node = BiReportScheduleWorker.class.getDeclaredField("node");
        node.setAccessible(true);
        String workerId = (String) node.get(worker);
        db.update("""
            INSERT INTO bi_report_schedules(id,report_id,owner_subject,cron_expression,timezone,enabled,next_run_at,
                                            lease_owner,lease_until)
            VALUES(?,?,?,'0 0 8 * * *','Europe/Istanbul',TRUE,now()-interval '1 minute',?,now()+interval '2 minutes')
            """, schedule, report, "report-owner", workerId);

        Method execute = BiReportScheduleWorker.class.getDeclaredMethod("execute", UUID.class);
        execute.setAccessible(true);
        execute.invoke(worker, schedule);

        assertThat(db.queryForObject("SELECT status FROM bi_report_schedule_runs WHERE schedule_id=?", String.class, schedule))
            .isEqualTo("SUCCEEDED");
        assertThat(db.queryForObject("SELECT row_count FROM bi_report_schedule_runs WHERE schedule_id=?", Integer.class, schedule))
            .isEqualTo(1);
        assertThat(db.queryForObject("SELECT lease_owner FROM bi_report_schedules WHERE id=?", String.class, schedule)).isNull();
        assertThat(db.queryForObject("SELECT result_json FROM bi_report_schedule_runs WHERE schedule_id=?", String.class, schedule))
            .contains("west");
    }
}
