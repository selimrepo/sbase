package io.sbase.server.lakehouse;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.spark.sql.SparkSession;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Multi-instance maintenance scheduler. PostgreSQL SKIP LOCKED claims due policies;
 * lease owner + monotonically increasing fencing token prevents stale workers
 * from committing a result after a lease has been stolen.
 */
public final class LakehouseMaintenanceScheduler implements AutoCloseable {
    private record Policy(UUID id,UUID workspace,String catalog,String namespace,String table,
                          String operation,int retentionDays,int orphanHours,String createdBy,UUID owner,long fence) {}
    private final HikariDataSource ds;
    private final IcebergMaintenanceService maintenance;
    private final LakehouseControlPlane control;
    private final ScheduledExecutorService executor;
    private final ScheduledExecutorService heartbeatExecutor;
    private final UUID owner=UUID.randomUUID();
    private final AtomicBoolean started=new AtomicBoolean();

    public LakehouseMaintenanceScheduler(SparkSession spark) {
        HikariConfig c=new HikariConfig();
        c.setJdbcUrl(env("SBASE_POSTGRES_URL","jdbc:postgresql://localhost:5432/sbase"));
        c.setUsername(env("SBASE_POSTGRES_USER","sbase"));
        c.setPassword(env("SBASE_POSTGRES_PASSWORD","change-me"));
        c.setMaximumPoolSize(3);c.setMinimumIdle(1);c.setPoolName("sbase-lakehouse-maintenance");
        ds=new HikariDataSource(c);maintenance=new IcebergMaintenanceService(spark);control=new LakehouseControlPlane(ds);
        executor=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"sbase-lakehouse-maintenance");t.setDaemon(true);return t;});
        heartbeatExecutor=Executors.newScheduledThreadPool(2,r->{Thread t=new Thread(r,"sbase-lakehouse-maintenance-heartbeat");t.setDaemon(true);return t;});
    }

    public void start() {
        if(started.compareAndSet(false,true))
            executor.scheduleWithFixedDelay(this::tickSafely,5,30,TimeUnit.SECONDS);
    }

    private void tickSafely() {
        try {
            for(Policy p:claimDue(8)) execute(p);
        } catch(Throwable error) { System.err.println("Lakehouse maintenance scheduler cycle failed: "+error); }
    }

    private List<Policy> claimDue(int limit) throws SQLException {
        String sql="""
          WITH due AS (
            SELECT id FROM lakehouse_maintenance_policy
            WHERE enabled=true AND next_run_at<=now()
              AND (lease_until IS NULL OR lease_until<now())
            ORDER BY next_run_at
            LIMIT ? FOR UPDATE SKIP LOCKED
          )
          UPDATE lakehouse_maintenance_policy p
          SET lease_owner=?, lease_until=now()+interval '5 minutes',
              fencing_token=p.fencing_token+1, last_started_at=now(),
              next_run_at=now()+(p.interval_seconds * interval '1 second')
          FROM due WHERE p.id=due.id
          RETURNING p.id,p.workspace_id,p.catalog_name,p.namespace_name,p.table_name,
                    p.operation,p.retention_days,p.orphan_older_than_hours,p.created_by,p.lease_owner,p.fencing_token
        """;
        try(Connection c=ds.getConnection()) {
            c.setAutoCommit(false);
            try(PreparedStatement s=c.prepareStatement(sql)) {
                s.setInt(1,Math.max(1,limit));s.setObject(2,owner);
                List<Policy> result=new ArrayList<>();
                try(ResultSet r=s.executeQuery()) {
                    while(r.next()) result.add(new Policy(r.getObject(1,UUID.class),r.getObject(2,UUID.class),
                            r.getString(3),r.getString(4),r.getString(5),r.getString(6),
                            r.getInt(7),r.getInt(8),r.getString(9),r.getObject(10,UUID.class),r.getLong(11)));
                }
                c.commit();return result;
            } catch(SQLException e) { c.rollback();throw e; }
        }
    }

    private void execute(Policy p) {
        ScheduledFuture<?> heartbeat=heartbeatExecutor.scheduleAtFixedRate(()->renewLease(p),30,30,TimeUnit.SECONDS);
        try { executeWithHeartbeat(p); }
        finally { heartbeat.cancel(false); }
    }

    private void renewLease(Policy p) {
        try(Connection c=ds.getConnection();PreparedStatement s=c.prepareStatement(
                "UPDATE lakehouse_maintenance_policy SET lease_until=now()+interval '5 minutes' WHERE id=? AND lease_owner=? AND fencing_token=? AND lease_until>now()")) {
            s.setObject(1,p.id());s.setObject(2,p.owner());s.setLong(3,p.fence());
            if(s.executeUpdate()==0) System.err.println("Lakehouse maintenance lease lost for policy "+p.id());
        } catch(SQLException e) { System.err.println("Lakehouse maintenance heartbeat failed for "+p.id()+": "+e); }
    }

    private void executeWithHeartbeat(Policy p) {
        UUID run=UUID.randomUUID();
        try(Connection c=ds.getConnection()) {
            try(PreparedStatement s=c.prepareStatement(
                    "INSERT INTO lakehouse_maintenance_run(id,policy_id,status) VALUES(?,?,'RUNNING')")) {
                s.setObject(1,run);s.setObject(2,p.id());s.executeUpdate();
            }
        } catch(SQLException e) { release(p);return; }

        String failure=null;
        try {
            if(p.table()==null || p.table().isBlank()) throw new IllegalArgumentException("Maintenance policy must target a table");
            if(p.createdBy()==null || p.createdBy().isBlank()) throw new SecurityException("Maintenance policy creator is missing");
            control.requireTable(new LakehouseControlPlane.Principal(p.createdBy(),p.workspace(),Set.of(),null),
                    p.catalog(),p.namespace(),p.table(),LakehouseControlPlane.Privilege.MODIFY);
            switch(p.operation()) {
                case "COMPACT" -> maintenance.compact(p.catalog(),p.namespace(),p.table(),128);
                case "REWRITE_MANIFESTS" -> maintenance.rewriteManifests(p.catalog(),p.namespace(),p.table());
                case "EXPIRE_SNAPSHOTS" -> maintenance.expireSnapshots(p.catalog(),p.namespace(),p.table(),10,p.retentionDays());
                case "REMOVE_ORPHAN_FILES" -> maintenance.removeOrphanFiles(p.catalog(),p.namespace(),p.table(),p.orphanHours());
                default -> throw new IllegalArgumentException("Unsupported maintenance operation "+p.operation());
            }
        } catch(Throwable e) { failure=e.getClass().getSimpleName()+": "+String.valueOf(e.getMessage()); }

        try(Connection c=ds.getConnection()) {
            c.setAutoCommit(false);
            try(PreparedStatement guard=c.prepareStatement(
                    "SELECT 1 FROM lakehouse_maintenance_policy WHERE id=? AND lease_owner=? AND fencing_token=? FOR UPDATE")) {
                guard.setObject(1,p.id());guard.setObject(2,p.owner());guard.setLong(3,p.fence());
                try(ResultSet r=guard.executeQuery()) {
                    if(!r.next()) { c.rollback();return; }
                }
            }
            try(PreparedStatement s=c.prepareStatement(
                    "UPDATE lakehouse_maintenance_run SET status=?,finished_at=now(),error_message=?,summary=?::jsonb WHERE id=?")) {
                s.setString(1,failure==null?"SUCCEEDED":"FAILED");s.setString(2,failure);
                s.setString(3,"{\"fencingToken\":"+p.fence()+"}");s.setObject(4,run);s.executeUpdate();
            }
            try(PreparedStatement s=c.prepareStatement(
                    "UPDATE lakehouse_maintenance_policy SET lease_owner=NULL,lease_until=NULL WHERE id=? AND lease_owner=? AND fencing_token=?")) {
                s.setObject(1,p.id());s.setObject(2,p.owner());s.setLong(3,p.fence());s.executeUpdate();
            }
            try(PreparedStatement s=c.prepareStatement(
                    "INSERT INTO lakehouse_maintenance_policy_audit(policy_id,event_type,fencing_token,details) VALUES(?,?,?,?::jsonb)")) {
                s.setObject(1,p.id());s.setString(2,failure==null?"SUCCEEDED":"FAILED");s.setLong(3,p.fence());
                s.setString(4,"{\"runId\":\""+run+"\",\"error\":"+(failure==null?"null":"\""+failure.replace("\"","'")+"\"")+"}");
                s.executeUpdate();
            }
            c.commit();
        } catch(SQLException e) { release(p); }
    }

    private void release(Policy p) {
        try(Connection c=ds.getConnection();PreparedStatement s=c.prepareStatement(
                "UPDATE lakehouse_maintenance_policy SET lease_owner=NULL,lease_until=NULL WHERE id=? AND lease_owner=? AND fencing_token=?")) {
            s.setObject(1,p.id());s.setObject(2,p.owner());s.setLong(3,p.fence());s.executeUpdate();
        } catch(SQLException ignored) { }
    }

    private static String env(String k,String d) { return Optional.ofNullable(System.getenv(k)).filter(v->!v.isBlank()).orElse(d); }
    @Override public void close() { executor.shutdownNow();heartbeatExecutor.shutdownNow();ds.close(); }
}
