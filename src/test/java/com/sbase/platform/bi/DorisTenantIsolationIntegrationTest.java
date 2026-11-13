package com.sbase.platform.bi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker=true)
class DorisTenantIsolationIntegrationTest {
    @Container static final PostgreSQLContainer<?> POSTGRES=new PostgreSQLContainer<>("postgres:16-alpine");

    @Test @EnabledIfEnvironmentVariable(named="SBASE_DORIS_TEST_JDBC_URL",matches=".+")
    void realDorisQueryIsTenantFilteredAndStaleOrInvalidOwnershipFailsClosed() throws Exception {
        String url=System.getenv("SBASE_DORIS_TEST_JDBC_URL");
        String user=System.getenv().getOrDefault("SBASE_DORIS_TEST_USERNAME","root");
        String password=System.getenv().getOrDefault("SBASE_DORIS_TEST_PASSWORD","");
        String database="sbase_tenant_"+UUID.randomUUID().toString().replace("-","").substring(0,12);
        UUID tenantA=UUID.randomUUID(),tenantB=UUID.randomUUID();
        DriverManagerDataSource pgSource=new DriverManagerDataSource(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword());
        JdbcTemplate pg=new JdbcTemplate(pgSource);
        DriverManagerDataSource dorisSource=new DriverManagerDataSource(url,user,password);
        JdbcTemplate doris=new JdbcTemplate(dorisSource);
        try {
            pg.execute("""
                CREATE TABLE doris_tenant_table_policies(
                  schema_name VARCHAR(128) NOT NULL,table_name VARCHAR(128) NOT NULL,tenant_column VARCHAR(64) NOT NULL,
                  enabled BOOLEAN NOT NULL,classification VARCHAR(24) NOT NULL,verification_status VARCHAR(16) NOT NULL,
                  verified_at TIMESTAMPTZ,invalid_tenant_rows BIGINT,verified_row_count BIGINT,
                  approved_by VARCHAR(200),approved_at TIMESTAMPTZ,updated_at TIMESTAMPTZ DEFAULT now(),
                  PRIMARY KEY(schema_name,table_name))
                """);
            pg.execute("""
                CREATE TABLE doris_tenant_policy_audit(
                  id BIGSERIAL PRIMARY KEY,schema_name VARCHAR(128) NOT NULL,table_name VARCHAR(128) NOT NULL,
                  tenant_id UUID,subject VARCHAR(200) NOT NULL,action VARCHAR(40) NOT NULL,
                  outcome VARCHAR(20) NOT NULL,occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),details JSONB NOT NULL)
                """);
            try(Connection connection=DriverManager.getConnection(url,user,password); Statement st=connection.createStatement()) {
                st.execute("CREATE DATABASE `"+database+"`");
                st.execute("CREATE TABLE `"+database+"`.events (id BIGINT NOT NULL, tenant_id VARCHAR(36) NOT NULL, payload VARCHAR(80)) " +
                    "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\",\"storage_medium\"=\"HDD\")");
                st.execute("INSERT INTO `"+database+"`.events VALUES (1,'"+tenantA+"','tenant-a'),(2,'"+tenantB+"','tenant-b')");
            }
            pg.update("""
                INSERT INTO doris_tenant_table_policies(schema_name,table_name,tenant_column,enabled,classification,
                    verification_status,verified_at,invalid_tenant_rows,verified_row_count,approved_by,approved_at)
                VALUES(?,'events','tenant_id',true,'TENANT_DATA','VERIFIED',now(),0,2,'integration-test',now())
                """,database);
            DorisTenantTablePolicy policy=new DorisTenantTablePolicy(pg,doris,database+".events",database);
            DorisBiQueryService query=new DorisBiQueryService(doris,policy);
            var rows=query.query(tenantA,"SELECT id, payload FROM "+database+".events");
            assertEquals(1,rows.size());
            assertEquals("tenant-a",rows.get(0).get("payload"));
            assertThrows(IllegalArgumentException.class,()->query.query(tenantA,
                "SELECT id FROM "+database+".events WHERE tenant_id='"+tenantB+"'"));

            try(Connection connection=DriverManager.getConnection(url,user,password); Statement st=connection.createStatement()) {
                st.execute("INSERT INTO `"+database+"`.events VALUES (3,'not-a-tenant-uuid','invalid')");
            }
            DorisTenantGovernanceAuditService audit=new DorisTenantGovernanceAuditService(pg,doris,database);
            var result=audit.auditDatabase("integration-test");
            assertEquals(false,result.get("complete"));
            assertEquals(1,result.get("blockedCount"));
            assertEquals(1L,pg.queryForObject("SELECT invalid_tenant_rows FROM doris_tenant_table_policies WHERE schema_name=? AND table_name='events'",Long.class,database));
            assertThrows(org.springframework.security.access.AccessDeniedException.class,
                ()->policy.requireTenantScoped(database+".events"));
        } finally {
            try(Connection connection=DriverManager.getConnection(url,user,password); Statement st=connection.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS `"+database+"`");
            }
        }
    }
}
