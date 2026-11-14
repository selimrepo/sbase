package com.sbase.platform.bi;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.security.access.AccessDeniedException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
class PostgresTenantIsolationIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    void realPostgreSqlBiQueriesAreTenantFilteredAndUnverifiedTablesFailClosed() {
        DriverManagerDataSource source = new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        JdbcTemplate jdbc = new JdbcTemplate(source);
        jdbc.execute("""
            CREATE TABLE postgres_tenant_table_policies(
              schema_name VARCHAR(128) NOT NULL, table_name VARCHAR(128) NOT NULL,
              tenant_column VARCHAR(64) NOT NULL, enabled BOOLEAN NOT NULL,
              classification VARCHAR(24) NOT NULL, verification_status VARCHAR(16) NOT NULL,
              verified_at TIMESTAMPTZ, invalid_tenant_rows BIGINT,
              PRIMARY KEY(schema_name,table_name))
            """);
        jdbc.execute("""
            CREATE TABLE events(
              id BIGINT NOT NULL, tenant_id UUID NOT NULL, payload TEXT NOT NULL,
              PRIMARY KEY(tenant_id,id))
            """);
        UUID tenantA = UUID.randomUUID(), tenantB = UUID.randomUUID();
        jdbc.update("INSERT INTO events(id,tenant_id,payload) VALUES(1,?, 'tenant-a'),(2,?, 'tenant-b')", tenantA, tenantB);
        jdbc.update("""
            INSERT INTO postgres_tenant_table_policies
              (schema_name,table_name,tenant_column,enabled,classification,verification_status,verified_at,invalid_tenant_rows)
            VALUES('public','events','tenant_id',true,'TENANT_DATA','VERIFIED',now(),0)
            """);

        DorisTenantTablePolicy policy = new DorisTenantTablePolicy(jdbc, "public.events", "public");
        DorisBiQueryService queries = new DorisBiQueryService(jdbc, policy);

        var rowsA = queries.query(tenantA, "SELECT id,payload FROM events");
        var rowsB = queries.query(tenantB, "SELECT id,payload FROM events");
        assertEquals(1, rowsA.size());
        assertEquals("tenant-a", rowsA.get(0).get("payload"));
        assertEquals(1, rowsB.size());
        assertEquals("tenant-b", rowsB.get(0).get("payload"));

        jdbc.update("UPDATE postgres_tenant_table_policies SET enabled=false WHERE schema_name='public' AND table_name='events'");
        assertThrows(AccessDeniedException.class, () -> queries.query(tenantA, "SELECT id,payload FROM events"));
    }
}
