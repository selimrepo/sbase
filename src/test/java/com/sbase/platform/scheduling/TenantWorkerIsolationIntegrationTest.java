package com.sbase.platform.scheduling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import com.sbase.platform.ingestion.ConnectorExecutionService;
import com.sbase.platform.ingestion.DeadLetterManagementService;
import com.sbase.platform.ingestion.DurableRecordSink;
import com.sbase.pipeline.PipelineExecutionService;
import com.sbase.platform.bi.BiWorkspaceService;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.server.ResponseStatusException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@Testcontainers(disabledWithoutDocker = true)
class TenantWorkerIsolationIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES=new PostgreSQLContainer<>("postgres:16-alpine");

    private HikariDataSource dataSource() {
        HikariDataSource ds=new HikariDataSource();
        ds.setJdbcUrl(POSTGRES.getJdbcUrl()); ds.setUsername(POSTGRES.getUsername()); ds.setPassword(POSTGRES.getPassword());
        return ds;
    }

    @Test
    void durableSinkRejectsRowsWhoseTenantDoesNotMatchConnectorOwnership() {
        try(HikariDataSource ds=dataSource()) {
            JdbcTemplate db=new JdbcTemplate(ds);
            db.execute("CREATE TABLE ingestion_connectors(id UUID PRIMARY KEY,tenant_id UUID NOT NULL)");
            db.execute("""
                CREATE TABLE ingestion_records(
                  id UUID PRIMARY KEY,connector_id UUID NOT NULL,tenant_id UUID NOT NULL,
                  source_key TEXT NOT NULL,payload JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL,
                  UNIQUE(connector_id,source_key))
                """);
            db.execute("""
                CREATE TABLE ingestion_checkpoints(
                  connector_id UUID NOT NULL,tenant_id UUID NOT NULL,checkpoint_key TEXT NOT NULL,
                  checkpoint_value TEXT NOT NULL,updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY(connector_id,checkpoint_key))
                """);
            UUID tenantA=UUID.randomUUID(),tenantB=UUID.randomUUID(),connectorA=UUID.randomUUID();
            db.update("INSERT INTO ingestion_connectors VALUES(?,?)",connectorA,tenantA);
            DurableRecordSink sink=new DurableRecordSink(db,new TransactionTemplate(new DataSourceTransactionManager(ds)),new ObjectMapper());
            var valid=new DurableRecordSink.Envelope("row-1",Map.of("tenant_id",tenantA.toString(),"value","owned"));
            assertEquals(1L,sink.write(connectorA,"test",List.of(valid),null,null));
            var foreign=new DurableRecordSink.Envelope("row-2",Map.of("tenant_id",tenantB.toString(),"value","foreign"));
            assertThrows(AccessDeniedException.class,()->sink.write(connectorA,"test",List.of(foreign),null,null));
            var missing=new DurableRecordSink.Envelope("row-3",Map.of("value","unowned"));
            assertThrows(AccessDeniedException.class,()->sink.write(connectorA,"test",List.of(missing),null,null));
            assertThat(db.queryForObject("SELECT count(*) FROM ingestion_records WHERE connector_id=?",Integer.class,connectorA)).isEqualTo(1);
        }
    }

    @Test
    void ingestionConnectorAndDeadLetterReadsCannotCrossTenantBoundary() {
        try(HikariDataSource ds=dataSource()) {
            JdbcTemplate db=new JdbcTemplate(ds);
            db.execute("CREATE TABLE ingestion_connectors(id UUID PRIMARY KEY,tenant_id UUID NOT NULL,connector_type TEXT NOT NULL,enabled BOOLEAN NOT NULL)");
            db.execute("""
                CREATE TABLE ingestion_dead_letters(
                  id UUID PRIMARY KEY,run_id UUID NOT NULL,connector_id UUID NOT NULL,tenant_id UUID NOT NULL,
                  connector_type TEXT NOT NULL,source_ref TEXT,error_message TEXT NOT NULL,attempt_count INT NOT NULL,
                  replay_attempts INT NOT NULL DEFAULT 0,last_replay_error TEXT,created_at TIMESTAMPTZ NOT NULL,
                  replayed_at TIMESTAMPTZ,replay_started_at TIMESTAMPTZ,payload JSONB NOT NULL)
                """);
            UUID tenantA=UUID.randomUUID(),tenantB=UUID.randomUUID(),connectorA=UUID.randomUUID(),connectorB=UUID.randomUUID();
            UUID letterA=UUID.randomUUID(),letterB=UUID.randomUUID();
            db.update("INSERT INTO ingestion_connectors VALUES(?,?,?,true)",connectorA,tenantA,"file");
            db.update("INSERT INTO ingestion_connectors VALUES(?,?,?,true)",connectorB,tenantB,"file");
            db.update("INSERT INTO ingestion_dead_letters(id,run_id,connector_id,tenant_id,connector_type,error_message,attempt_count,created_at,payload) VALUES(?,?,?,?,?,'failed',1,now(),'{}')",
                letterA,UUID.randomUUID(),connectorA,tenantA,"file");
            db.update("INSERT INTO ingestion_dead_letters(id,run_id,connector_id,tenant_id,connector_type,error_message,attempt_count,created_at,payload) VALUES(?,?,?,?,?,'failed',1,now(),'{}')",
                letterB,UUID.randomUUID(),connectorB,tenantB,"file");

            ConnectorExecutionService executions=new ConnectorExecutionService(List.of(connector("file")),db,new ObjectMapper(),2,Runnable::run);
            DeadLetterManagementService deadLetters=new DeadLetterManagementService(db,new ObjectMapper(),executions);
            assertThat(deadLetters.pending(100,tenantA)).extracting(row->row.get("id")).containsExactly(letterA);
            assertThat(deadLetters.pending(100,tenantB)).extracting(row->row.get("id")).containsExactly(letterB);
            assertThrows(IllegalStateException.class,()->deadLetters.replay(letterA,tenantB,"bob"));
            assertThrows(ResponseStatusException.class,()->executions.execute("file",connectorA,null,
                "source","target",Map.of(),"foreign-key",tenantB,"bob"));
            assertThat(db.queryForObject("SELECT replayed_at IS NULL FROM ingestion_dead_letters WHERE id=?",Boolean.class,letterA)).isTrue();
        }
    }

    @Test
    void distributedSchedulerRejectsForeignPipelineAndCrossTenantDependencies() {
        try(HikariDataSource ds=dataSource()) {
            JdbcTemplate db=new JdbcTemplate(ds);
            db.execute("CREATE TABLE pipelines(id UUID PRIMARY KEY,tenant_id UUID NOT NULL)");
            db.execute("""
                CREATE TABLE scheduled_jobs(
                  id UUID PRIMARY KEY,tenant_id UUID NOT NULL,name TEXT NOT NULL,created_by TEXT NOT NULL,
                  pipeline_id UUID,cron_expression TEXT NOT NULL,timezone TEXT,calendar_json TEXT,
                  failure_policy TEXT,misfire_policy TEXT,next_run_at TIMESTAMPTZ NOT NULL,
                  attempt_count INT NOT NULL DEFAULT 0,max_attempts INT NOT NULL DEFAULT 4,
                  misfire_grace_seconds INT NOT NULL DEFAULT 60,enabled BOOLEAN NOT NULL DEFAULT true,
                  lease_until TIMESTAMPTZ,lease_owner TEXT,depends_on UUID,last_success_at TIMESTAMPTZ,
                  last_run_at TIMESTAMPTZ,created_at TIMESTAMPTZ NOT NULL,updated_at TIMESTAMPTZ)
                """);
            db.execute("CREATE TABLE schedule_runs(schedule_id UUID NOT NULL,tenant_id UUID NOT NULL,status TEXT NOT NULL)");
            UUID tenantA=UUID.randomUUID(),tenantB=UUID.randomUUID();
            UUID pipelineA=UUID.randomUUID(),pipelineB=UUID.randomUUID();
            UUID scheduleA=UUID.randomUUID(),scheduleB=UUID.randomUUID(),foreignPipeline=UUID.randomUUID(),foreignDependency=UUID.randomUUID();
            db.update("INSERT INTO pipelines VALUES(?,?)",pipelineA,tenantA);
            db.update("INSERT INTO pipelines VALUES(?,?)",pipelineB,tenantB);
            Instant due=Instant.now().minusSeconds(5);
            insertSchedule(db,scheduleA,tenantA,pipelineA,null,due);
            insertSchedule(db,scheduleB,tenantB,pipelineB,null,due);
            insertSchedule(db,foreignPipeline,tenantA,pipelineB,null,due);
            insertSchedule(db,foreignDependency,tenantA,pipelineA,scheduleB,due);
            db.update("UPDATE scheduled_jobs SET last_success_at=now() WHERE id=?",scheduleB);

            DistributedScheduleWorker worker=new DistributedScheduleWorker(db,Runnable::run,
                mock(PipelineExecutionService.class),new ObjectMapper());
            Set<UUID> claimed=Set.copyOf(worker.claim());
            assertThat(claimed).contains(scheduleA,scheduleB).doesNotContain(foreignPipeline,foreignDependency);
            assertThat(db.queryForObject("SELECT count(*) FROM scheduled_jobs WHERE id IN (?,?) AND lease_owner IS NOT NULL",
                Integer.class,scheduleA,scheduleB)).isEqualTo(2);
        }
    }

    private static IngestionConnector connector(String type) {
        IngestionConnector connector=mock(IngestionConnector.class);
        when(connector.type()).thenReturn(type);
        return connector;
    }

    private static void insertSchedule(JdbcTemplate db,UUID id,UUID tenant,UUID pipeline,UUID dependsOn,Instant due) {
        db.update("""
            INSERT INTO scheduled_jobs(id,tenant_id,name,created_by,pipeline_id,cron_expression,timezone,calendar_json,
              failure_policy,misfire_policy,next_run_at,enabled,depends_on,created_at,updated_at)
            VALUES(?,?,?,'system',?,'0 * * * * *','UTC','{}','RETRY','RUN_ONCE',?,true,?,now(),now())
            """,id,tenant,"schedule-"+id,pipeline,Timestamp.from(due),dependsOn);
    }
}
