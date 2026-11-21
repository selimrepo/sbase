package io.sbase.server.lakehouse;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;

import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LakehouseControlPlanePostgresTest {
    private static HikariDataSource dataSource;
    private static LakehouseControlPlane control;
    private static UUID workspace;
    private static String namespace;

    @BeforeAll static void setup() {
        String url=System.getenv("SBASE_TEST_POSTGRES_URL");
        Assumptions.assumeTrue(url!=null && !url.isBlank(),"PostgreSQL integration environment is not configured");
        HikariConfig config=new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(System.getenv().getOrDefault("SBASE_TEST_POSTGRES_USER","sbase"));
        config.setPassword(System.getenv().getOrDefault("SBASE_TEST_POSTGRES_PASSWORD","sbase-test"));
        config.setMaximumPoolSize(3);
        dataSource=new HikariDataSource(config);
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        control=new LakehouseControlPlane(dataSource);
        workspace=UUID.randomUUID();
        namespace="tenant_"+workspace.toString().replace("-","").substring(0,12);
        try(var c=dataSource.getConnection()) {
            try(var p=c.prepareStatement("INSERT INTO workspace(id,name) VALUES(?,?)")) {
                p.setObject(1,workspace);p.setString(2,"Lakehouse RBAC integration test");p.executeUpdate();
            }
            try(var p=c.prepareStatement("INSERT INTO lakehouse_namespace_owner(catalog_name,namespace_name,workspace_id,owner_principal) VALUES('sbase',?,?,?)")) {
                p.setString(1,namespace);p.setObject(2,workspace);p.setString(3,"owner");p.executeUpdate();
            }
        } catch(Exception e) { throw new RuntimeException(e); }
    }

    @AfterAll static void cleanup() throws Exception {
        if(dataSource==null) return;
        try(var c=dataSource.getConnection();var p=c.prepareStatement("DELETE FROM workspace WHERE id=?")) {
            p.setObject(1,workspace);p.executeUpdate();
        } finally { dataSource.close(); }
    }

    @Test void deadLetterReplayLeaseFencesExpiredWorkers() throws Exception {
        UUID runId=UUID.randomUUID();
        try(var connection=dataSource.getConnection();
            var statement=connection.prepareStatement("""
                INSERT INTO lakehouse_ingestion_run(id,workspace_id,target_namespace,target_table,engine,status)
                VALUES(?,?,?,'dead_letter_test','SPARK','FAILED')
                """)) {
            statement.setObject(1,runId);statement.setObject(2,workspace);statement.setString(3,namespace);
            statement.executeUpdate();
        }
        UUID recordId=control.enqueueDeadLetter(workspace,runId,"0","{\"offset\":1}","key-1",
                "{\"value\":42}","TestFailure","simulated failure");
        var first=control.claimDeadLetters(workspace,runId,10);
        assertEquals(1,first.size());
        var oldLease=first.get(0);
        assertNotNull(oldLease.leaseToken());
        try(var connection=dataSource.getConnection();
            var statement=connection.prepareStatement(
                "UPDATE lakehouse_dead_letter SET lease_until=now()-interval '1 second' WHERE id=?")) {
            statement.setObject(1,recordId);assertEquals(1,statement.executeUpdate());
        }
        var second=control.claimDeadLetters(workspace,runId,10);
        assertEquals(1,second.size(),"expired claims should be recoverable");
        var newLease=second.get(0);
        assertNotEquals(oldLease.leaseToken(),newLease.leaseToken(),"reclaimed work must receive a new fencing token");
        assertEquals(2,newLease.attempts());
        assertThrows(IllegalStateException.class,
                ()->control.finishDeadLetter(recordId,oldLease.leaseToken(),true));
        control.finishDeadLetter(recordId,newLease.leaseToken(),true);
        try(var connection=dataSource.getConnection();
            var statement=connection.prepareStatement("SELECT status FROM lakehouse_dead_letter WHERE id=?")) {
            statement.setObject(1,recordId);
            try(var rows=statement.executeQuery()) { assertTrue(rows.next());assertEquals("RESOLVED",rows.getString(1)); }
        }
    }

    @Test void groupAndTableGrantsAreEnforcedAndRevocable() {
        var analyst=new LakehouseControlPlane.Principal("analyst-1",workspace,Set.of("analytics"),null);
        assertThrows(SecurityException.class,()->control.requireNamespace(analyst,"sbase",namespace,LakehouseControlPlane.Privilege.USE));
        control.grantNamespace("sbase",namespace,LakehouseControlPlane.PrincipalType.GROUP,"analytics",
                LakehouseControlPlane.Privilege.USE,"owner",null);
        control.grantTable("sbase",namespace,"orders",LakehouseControlPlane.PrincipalType.GROUP,"analytics",
                LakehouseControlPlane.Privilege.SELECT,"owner",null);
        assertDoesNotThrow(()->control.requireTable(analyst,"sbase",namespace,"orders",LakehouseControlPlane.Privilege.SELECT));
        assertThrows(SecurityException.class,()->control.requireTable(analyst,"sbase",namespace,"payroll",LakehouseControlPlane.Privilege.SELECT));
        assertTrue(control.revokeTable("sbase",namespace,"orders",LakehouseControlPlane.PrincipalType.GROUP,"analytics",
                LakehouseControlPlane.Privilege.SELECT));
        assertThrows(SecurityException.class,()->control.requireTable(analyst,"sbase",namespace,"orders",LakehouseControlPlane.Privilege.SELECT));
    }

    @Test void userGrantCanBeRevokedWithoutAffectingAnotherWorkspace() {
        control.grantNamespace("sbase",namespace,LakehouseControlPlane.PrincipalType.USER,"reader",
                LakehouseControlPlane.Privilege.USE,"owner",null);
        control.grantTable("sbase",namespace,"events",LakehouseControlPlane.PrincipalType.USER,"reader",
                LakehouseControlPlane.Privilege.SELECT,"owner",null);
        var reader=new LakehouseControlPlane.Principal("reader",workspace,Set.of(),null);
        assertDoesNotThrow(()->control.requireTable(reader,"sbase",namespace,"events",LakehouseControlPlane.Privilege.SELECT));
        var otherWorkspace=new LakehouseControlPlane.Principal("reader",UUID.randomUUID(),Set.of(),null);
        assertThrows(SecurityException.class,()->control.requireTable(otherWorkspace,"sbase",namespace,"events",LakehouseControlPlane.Privilege.SELECT));
        assertTrue(control.revokeNamespace("sbase",namespace,LakehouseControlPlane.PrincipalType.USER,"reader",
                LakehouseControlPlane.Privilege.USE));
        assertThrows(SecurityException.class,()->control.requireTable(reader,"sbase",namespace,"events",LakehouseControlPlane.Privilege.SELECT));
    }

    @Test void expiredGrantAndCrossWorkspaceAccessAreDenied() {
        control.grantNamespace("sbase",namespace,LakehouseControlPlane.PrincipalType.USER,"expired-user",
                LakehouseControlPlane.Privilege.USE,"owner",Instant.now().minusSeconds(30));
        var expired=new LakehouseControlPlane.Principal("expired-user",workspace,Set.of(),null);
        assertThrows(SecurityException.class,()->control.requireNamespace(expired,"sbase",namespace,LakehouseControlPlane.Privilege.USE));
        var foreign=new LakehouseControlPlane.Principal("analyst-1",UUID.randomUUID(),Set.of("analytics"),null);
        assertThrows(SecurityException.class,()->control.requireNamespace(foreign,"sbase",namespace,LakehouseControlPlane.Privilege.USE));
    }
}
