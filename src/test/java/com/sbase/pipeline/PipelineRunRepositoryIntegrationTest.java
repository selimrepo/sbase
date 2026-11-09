package com.sbase.pipeline;

import com.sbase.platform.feature.OnlineFeatureStoreService;

import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.Instant;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class PipelineRunRepositoryIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("sbase")
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

    @Autowired PipelineRepository pipelines;
    @Autowired PipelineRunRepository runs;
    @Autowired JdbcTemplate jdbc;

    @Test
    void persistsImmutablePipelineSnapshotAndExecutionLifecycle() {
        PipelineEntity pipeline = pipelines.saveAndFlush(new PipelineEntity(
                "integration pipeline", "repository integration test",
                "{\"steps\":[{\"id\":\"source\",\"type\":\"doris_read\",\"table\":\"analytics.events\"}]}",
                true));
        PipelineRunEntity run = runs.saveAndFlush(new PipelineRunEntity(
                pipeline.getId(), pipeline.getVersion(), pipeline.getDefinitionJson()));

        run.start();
        runs.saveAndFlush(run);
        run.succeed("{\"recordsProcessed\":3}", 3);
        runs.saveAndFlush(run);

        PipelineRunEntity reloaded = runs.findById(run.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo("SUCCEEDED");
        assertThat(reloaded.getPipelineVersion()).isEqualTo(1);
        assertThat(reloaded.getDefinitionSnapshot()).contains("doris_read");
        assertThat(reloaded.getRecordsProcessed()).isEqualTo(3);
        assertThat(reloaded.getStartedAt()).isNotNull();
        assertThat(reloaded.getFinishedAt()).isNotNull();
    }

    @Test
    void persistsRetryScheduleAndClaimsEachAttemptOnlyOnce() {
        PipelineEntity pipeline = pipelines.saveAndFlush(new PipelineEntity(
            "retry pipeline", "retry persistence test",
            "{\"steps\":[{\"id\":\"source\",\"type\":\"doris_read\",\"table\":\"analytics.events\"}]}", true));
        PipelineRunEntity run = new PipelineRunEntity(pipeline.getId(), pipeline.getVersion(), pipeline.getDefinitionJson());
        run.setMaxAttempts(3);
        runs.saveAndFlush(run);
        assertThat(runs.claimQueued(run.getId())).isEqualTo(1);
        PipelineRunEntity running = runs.findById(run.getId()).orElseThrow();
        running.scheduleRetry("temporary Doris failure", java.time.Instant.now().minusSeconds(1));
        runs.saveAndFlush(running);

        assertThat(runs.findTop100ByStatusAndNextAttemptAtLessThanEqualOrderByNextAttemptAtAsc(
            "RETRY_WAIT", java.time.Instant.now())).hasSize(1);
        assertThat(runs.claimDueRetry(run.getId())).isEqualTo(1);
        assertThat(runs.claimDueRetry(run.getId())).isZero();
        assertThat(runs.findById(run.getId()).orElseThrow().getStatus()).isEqualTo("QUEUED");
    }


    @Test
    void onlineFeatureUpsertServesNewestEventAndIgnoresLateArrivals() {
        OnlineFeatureStoreService features = new OnlineFeatureStoreService(jdbc, new ObjectMapper());
        features.upsert("customer_profile_it", "customer-7", 1,
            Instant.parse("2026-11-08T12:00:00Z"), Map.of("score", 0.95));
        features.upsert("customer_profile_it", "customer-7", 1,
            Instant.parse("2026-11-08T11:00:00Z"), Map.of("score", 0.10));
        assertThat(features.get("customer_profile_it", "customer-7")).isPresent();
        assertThat(features.get("customer_profile_it", "customer-7").orElseThrow())
            .containsEntry("score", 0.95);
    }


    @Test
    void recoversExpiredRunningLeaseIntoDurableRetryQueue() {
        PipelineEntity pipeline = pipelines.saveAndFlush(new PipelineEntity(
            "lease recovery", "expired execution lease",
            "{\"steps\":[{\"id\":\"source\",\"type\":\"doris_read\",\"table\":\"analytics.events\"}]}", true));
        PipelineRunEntity run = runs.saveAndFlush(new PipelineRunEntity(
            pipeline.getId(), pipeline.getVersion(), pipeline.getDefinitionJson()));
        assertThat(runs.claimQueued(run.getId())).isEqualTo(1);
        jdbc.update("UPDATE pipeline_runs SET lease_until=now()-interval '1 second' WHERE id=?", run.getId());
        assertThat(runs.recoverExpired(run.getId())).isEqualTo(1);
        PipelineRunEntity recovered = runs.findById(run.getId()).orElseThrow();
        assertThat(recovered.getStatus()).isEqualTo("RETRY_WAIT");
        assertThat(recovered.getNextAttemptAt()).isNotNull();
        assertThat(runs.recoverExpired(run.getId())).isZero();
    }

}
