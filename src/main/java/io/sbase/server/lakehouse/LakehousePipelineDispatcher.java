package io.sbase.server.lakehouse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.sbase.server.runtime.PlatformRuntimeService;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PostgreSQL-backed multi-node pipeline dispatcher. Claims are serialized with
 * SKIP LOCKED and fenced on completion. Spark batch jobs are at-least-once;
 * Flink streaming jobs use checkpointed EXACTLY_ONCE sinks.
 */
public final class LakehousePipelineDispatcher implements AutoCloseable {
    private static final ObjectMapper JSON=new ObjectMapper();
    private static final TypeReference<Map<String,Object>> MAP=new TypeReference<>() {};
    private record Job(UUID attemptId,UUID pipelineId,UUID runId,UUID workspaceId,
                       int attempt,int maxRetries,UUID owner,long fence,String definition) {}
    private final HikariDataSource ds;
    private final SparkSession spark;
    private final ScheduledExecutorService executor;
    private final UUID owner=UUID.randomUUID();
    private final AtomicBoolean started=new AtomicBoolean();

    public LakehousePipelineDispatcher(SparkSession spark) {
        this.spark=spark;
        HikariConfig c=new HikariConfig();
        c.setJdbcUrl(env("SBASE_POSTGRES_URL","jdbc:postgresql://localhost:5432/sbase"));
        c.setUsername(env("SBASE_POSTGRES_USER","sbase"));c.setPassword(env("SBASE_POSTGRES_PASSWORD","change-me"));
        c.setMaximumPoolSize(4);c.setMinimumIdle(1);c.setPoolName("sbase-lakehouse-pipeline-dispatcher");
        ds=new HikariDataSource(c);
        executor=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"sbase-lakehouse-pipeline-dispatcher");t.setDaemon(true);return t;});
    }

    public void start() {
        if(started.compareAndSet(false,true)) executor.scheduleWithFixedDelay(this::tickSafely,3,5,TimeUnit.SECONDS);
    }

    private void tickSafely() {
        try { for(Job job:claim(4)) process(job); }
        catch(Throwable e) { System.err.println("Lakehouse pipeline dispatch cycle failed: "+e); }
    }

    private List<Job> claim(int limit) throws SQLException {
        String sql="""
          WITH due AS (
            SELECT a.id FROM lakehouse_pipeline_attempt a
            JOIN lakehouse_ingestion_pipeline p ON p.id=a.pipeline_id
            WHERE p.enabled=true AND (
              (a.status IN ('QUEUED','RETRY_WAIT') AND a.next_attempt_at<=now())
              OR (a.status='RUNNING' AND a.lease_until<now())
            )
            ORDER BY a.submitted_at LIMIT ? FOR UPDATE OF a SKIP LOCKED
          )
          UPDATE lakehouse_pipeline_attempt a
          SET status='RUNNING',attempt=a.attempt+1,lease_owner=?,lease_until=now()+interval '2 hours',
              heartbeat_at=now(),fencing_token=a.fencing_token+1,started_at=COALESCE(a.started_at,now())
          FROM due d,lakehouse_ingestion_pipeline p,lakehouse_pipeline_version v
          WHERE a.id=d.id AND p.id=a.pipeline_id AND v.pipeline_id=a.pipeline_id AND v.version=a.pipeline_version
          RETURNING a.id,a.pipeline_id,a.ingestion_run_id,p.workspace_id,a.attempt,p.max_retries,
                    a.lease_owner,a.fencing_token,v.definition::text
        """;
        try(Connection c=ds.getConnection()) {
            c.setAutoCommit(false);
            try(PreparedStatement s=c.prepareStatement(sql)) {
                s.setInt(1,Math.max(1,limit));s.setObject(2,owner);
                List<Job> out=new ArrayList<>();
                try(ResultSet r=s.executeQuery()) {
                    while(r.next()) out.add(new Job(r.getObject(1,UUID.class),r.getObject(2,UUID.class),
                            r.getObject(3,UUID.class),r.getObject(4,UUID.class),r.getInt(5),r.getInt(6),
                            r.getObject(7,UUID.class),r.getLong(8),r.getString(9)));
                }
                c.commit();return out;
            } catch(Exception e) { c.rollback();throw e; }
        }
    }

    private void process(Job job) {
        boolean longRunning=false;
        try {
            Map<String,Object> definition=JSON.readValue(job.definition(),MAP);
            String engine=Objects.toString(definition.get("engine"),"SPARK");
            if("FLINK".equalsIgnoreCase(engine)) {
                submitFlink(definition);
                longRunning=true;
            } else {
                runSpark(job,definition);
            }
            finish(job,longRunning? "RUNNING":"SUCCEEDED",null,longRunning);
        } catch(Throwable e) {
            fail(job,e);
        }
    }

    @SuppressWarnings("unchecked")
    private void runSpark(Job job,Map<String,Object> definition) {\n        try(LakehouseExecutionContext context=LakehouseExecutionContext.enter(job.workspaceId(),"pipeline:"+job.pipelineId())) {
        String sourceType=Objects.toString(definition.get("sourceType"),"");
        Map<String,String> source=stringOptions((Map<?,?>)definition.get("source"));
        String secretRef=Objects.toString(definition.get("secretRef"),"");
        Map<String,String> options=definition.get("configuration") instanceof Map<?,?> m
                ? stringOptions(m) : Map.of();
        LakehouseIngestionService ingestion=new LakehouseIngestionService(spark,ref->resolveSecret(ref));
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> data;
        switch(sourceType) {
            case "S3" -> data=ingestion.readObjectStorage(required(source,"uri"),source.getOrDefault("format","parquet"),options);
            case "JDBC" -> data=ingestion.readJdbc(required(source,"jdbcUrl"),required(source,"tableOrQuery"),
                    required(source,"user"),secretRef,source.get("driver"),options);
            case "KAFKA" -> data=ingestion.readKafkaBatch(required(source,"bootstrapServers"),required(source,"topic"),
                    source.getOrDefault("startingOffsets","earliest"),options);
            default -> throw new IllegalArgumentException("Unsupported Spark ingestion source "+sourceType);
        }
        ingestion.appendIceberg(data,"sbase",required(definition,"targetNamespace"),required(definition,"targetTable"));\n        }\n    }\n\n    private void submitFlink(Map<String,Object> definition) throws Exception {
        Map<String,Object> config=definition.get("configuration") instanceof Map<?,?> m ? objectMap(m) : new LinkedHashMap<>();
        String jarId=required(config,"flinkJarId");
        String sourceDdl=required(config,"sourceDdl");
        String insertSql=required(config,"insertSql");
        if(!sourceDdl.stripLeading().toUpperCase(Locale.ROOT).startsWith("CREATE TABLE") ||
           !insertSql.stripLeading().toUpperCase(Locale.ROOT).startsWith("INSERT INTO"))
            throw new IllegalArgumentException("Flink pipeline requires CREATE TABLE sourceDdl and INSERT INTO insertSql");
        String checkpoint=required(definition,"checkpointUri");
        Map<String,Object> args=Map.of("checkpointUri",checkpoint,"sourceDdl",sourceDdl,"insertSql",insertSql);
        Map<String,Object> submit=Map.of("entryClass","io.sbase.server.lakehouse.FlinkIcebergStreamingJob",
                "parallelism",config.getOrDefault("parallelism",1),"programArgs",JSON.writeValueAsString(args));
        new PlatformRuntimeService(spark).flinkSubmit(jarId,submit);
    }

    private void finish(Job job,String status,String error,boolean keepRunning) throws SQLException {
        try(Connection c=ds.getConnection()) {
            c.setAutoCommit(false);
            if(!ownsLease(c,job)) { c.rollback();return; }
            try(PreparedStatement p=c.prepareStatement(
                    "UPDATE lakehouse_pipeline_attempt SET status=?,finished_at=?,lease_owner=NULL,lease_until=NULL,heartbeat_at=NULL,error_class=?,error_message=? WHERE id=? AND lease_owner=? AND fencing_token=?")) {
                p.setString(1,status);p.setTimestamp(2,keepRunning?null:new Timestamp(System.currentTimeMillis()));
                p.setString(3,error);p.setString(4,error);p.setObject(5,job.attemptId());p.setObject(6,job.owner());p.setLong(7,job.fence());p.executeUpdate();
            }
            try(PreparedStatement p=c.prepareStatement("UPDATE lakehouse_ingestion_run SET status=?,started_at=COALESCE(started_at,now()),finished_at=? WHERE id=? AND workspace_id=?")) {
                p.setString(1,status);p.setTimestamp(2,keepRunning?null:new Timestamp(System.currentTimeMillis()));p.setObject(3,job.runId());p.setObject(4,job.workspaceId());p.executeUpdate();
            }
            c.commit();
        }
    }

    private void fail(Job job,Throwable error) {
        String message=String.valueOf(error.getMessage());
        try(Connection c=ds.getConnection()) {
            c.setAutoCommit(false);
            if(!ownsLease(c,job)) { c.rollback();return; }
            boolean retry=job.attempt()<=job.maxRetries();
            String status=retry?"RETRY_WAIT":"FAILED";
            try(PreparedStatement p=c.prepareStatement("""
              UPDATE lakehouse_pipeline_attempt SET status=?,next_attempt_at=now()+interval '30 seconds',
                lease_owner=NULL,lease_until=NULL,heartbeat_at=NULL,
                finished_at=CASE WHEN ? THEN NULL ELSE now() END,
                error_class=?,error_message=? WHERE id=? AND lease_owner=? AND fencing_token=?
            """)) {
                p.setString(1,status);p.setBoolean(2,retry);p.setString(3,error.getClass().getName());
                p.setString(4,message);p.setObject(5,job.attemptId());p.setObject(6,job.owner());p.setLong(7,job.fence());p.executeUpdate();
            }
            try(PreparedStatement p=c.prepareStatement("UPDATE lakehouse_ingestion_run SET status=?,error_message=?,finished_at=? WHERE id=? AND workspace_id=?")) {
                p.setString(1,retry?"QUEUED":"FAILED");p.setString(2,message);
                p.setTimestamp(3,retry?null:new Timestamp(System.currentTimeMillis()));p.setObject(4,job.runId());p.setObject(5,job.workspaceId());p.executeUpdate();
            }
            c.commit();
        } catch(SQLException e) { System.err.println("Could not persist pipeline failure: "+e); }
    }

    private boolean ownsLease(Connection c,Job job) throws SQLException {
        try(PreparedStatement p=c.prepareStatement("SELECT 1 FROM lakehouse_pipeline_attempt WHERE id=? AND lease_owner=? AND fencing_token=? FOR UPDATE")) {
            p.setObject(1,job.attemptId());p.setObject(2,job.owner());p.setLong(3,job.fence());
            try(ResultSet r=p.executeQuery()) { return r.next(); }
        }
    }

    private static Map<String,String> stringOptions(Map<?,?> map) {
        Map<String,String> out=new LinkedHashMap<>();
        if(map!=null) map.forEach((k,v)->{if(k instanceof String key && v!=null)out.put(key,String.valueOf(v));});
        return out;
    }
    private static Map<String,Object> objectMap(Map<?,?> map) {
        Map<String,Object> out=new LinkedHashMap<>();map.forEach((k,v)->{if(k instanceof String key)out.put(key,v);});return out;
    }
    private static String required(Map<String,?> map,String key) {
        String value=Objects.toString(map.get(key),"").trim();if(value.isEmpty())throw new IllegalArgumentException(key+" is required");return value;
    }
    private static String resolveSecret(String ref) {
        if(ref==null||ref.isBlank())throw new IllegalArgumentException("secretRef is required");
        String envKey="SBASE_SECRET_"+ref.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]","_");
        String value=System.getenv(envKey);
        if(value==null)throw new IllegalStateException("Secret provider did not resolve "+ref+" (expected environment-backed "+envKey+")");
        return value;
    }
    private static String env(String k,String d) { return Optional.ofNullable(System.getenv(k)).filter(v->!v.isBlank()).orElse(d); }
    @Override public void close() { executor.shutdownNow();ds.close(); }
}
