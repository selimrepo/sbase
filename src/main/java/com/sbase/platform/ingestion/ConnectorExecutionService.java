package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import java.time.Instant;
import java.util.*;

@Service
public class ConnectorExecutionService {
    private final Map<String,IngestionConnector> connectors;
    private final JdbcTemplate db;
    private final ObjectMapper mapper;
    public ConnectorExecutionService(List<IngestionConnector> list,@Qualifier("jdbcTemplate") JdbcTemplate db,ObjectMapper mapper) {
        this.db=db; this.mapper=mapper;
        Map<String,IngestionConnector> m=new HashMap<>();
        for(var c:list) if(m.put(c.type(),c)!=null) throw new IllegalStateException("Duplicate connector type: "+c.type());
        connectors=Map.copyOf(m);
    }
    public Map<String,Object> execute(String type,UUID connectorId,UUID datasetId,String source,String target,
            Map<String,String> options,String idempotencyKey,UUID tenantId,String subject) {
        var connector=connectors.get(type.toLowerCase(Locale.ROOT));
        if(connector==null) throw new IllegalArgumentException("Unsupported connector: "+type);
        if(connectorId==null) throw new IllegalArgumentException("connectorId is required");
        String registeredType=db.query("SELECT connector_type FROM ingestion_connectors WHERE id=? AND tenant_id=? AND enabled=true",
            rs->rs.next()?rs.getString(1):null,connectorId,tenantId);
        if(registeredType==null) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Connector not found");
        if(!registeredType.equalsIgnoreCase(type)) throw new ResponseStatusException(HttpStatus.CONFLICT,"Connector type does not match registration");
        if(datasetId!=null && !Boolean.TRUE.equals(db.query("SELECT EXISTS(SELECT 1 FROM datasets WHERE id=? AND tenant_id=?)",
            rs->rs.next()&&rs.getBoolean(1),datasetId,tenantId)))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Dataset not found");
        String key=idempotencyKey==null||idempotencyKey.isBlank()?null:idempotencyKey.trim();
        UUID run=UUID.randomUUID();
        int inserted=db.update("""
            INSERT INTO ingestion_runs(id,connector_id,dataset_id,tenant_id,submitted_by,status,started_at,idempotency_key)
            VALUES(?,?,?,?,?,'RUNNING',?,?)
            ON CONFLICT (tenant_id,idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            """,run,connectorId,datasetId,tenantId,subject,Instant.now(),key);
        if(inserted==0) {
            Map<String,Object> existing=db.queryForMap("SELECT id,status,records_read,records_written FROM ingestion_runs WHERE tenant_id=? AND idempotency_key=?",tenantId,key);
            return Map.of("runId",existing.get("id"),"status",existing.get("status"),"recordsRead",existing.get("records_read"),
                "recordsWritten",existing.get("records_written"),"replayed",true);
        }
        Map<String,String> effective=new HashMap<>(options==null?Map.of():options);
        effective.put("connectorId",connectorId.toString());
        int maxAttempts=parseBounded(effective.get("maxAttempts"),3,1,10);
        long backoff=parseBoundedLong(effective.get("retryBackoffMs"),1000,100,60000);
        RuntimeException failure=null;
        for(int attempt=1;attempt<=maxAttempts;attempt++) {
            try {
                var result=connector.ingest(new IngestionConnector.ConnectorRequest(source,target,Map.copyOf(effective)));
                db.update("UPDATE ingestion_runs SET status='SUCCEEDED',finished_at=now(),records_read=?,records_written=?,attempt_count=? WHERE id=? AND tenant_id=?",
                    result.recordsRead(),result.recordsWritten(),attempt,run,tenantId);
                return Map.of("runId",run,"status","SUCCEEDED","recordsRead",result.recordsRead(),"recordsWritten",result.recordsWritten(),
                    "attempts",attempt,"details",result.details(),"replayed",false);
            } catch(RuntimeException ex) {
                failure=ex;
                if(attempt<maxAttempts) {
                    try { Thread.sleep(Math.min(backoff*(1L<<(attempt-1)),60000)); }
                    catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); failure=new IllegalStateException("Ingestion retry interrupted",interrupted); break; }
                }
            }
        }
        String message=failure==null?"Unknown ingestion failure":String.valueOf(failure.getMessage());
        try {
            String config=mapper.writeValueAsString(Map.of("source",String.valueOf(source),"target",String.valueOf(target),"options",effective));
            db.update("""
                INSERT INTO ingestion_dead_letters(id,run_id,connector_id,tenant_id,connector_type,source_ref,payload,error_message,attempt_count,created_at)
                VALUES(?,?,?,?,?,?,?::jsonb,?,?,now())
                """,UUID.randomUUID(),run,connectorId,tenantId,type,source,config,message,maxAttempts);
        } catch(Exception dlqError) { if(failure!=null) failure.addSuppressed(dlqError); }
        db.update("UPDATE ingestion_runs SET status='FAILED',finished_at=now(),error_message=?,attempt_count=? WHERE id=? AND tenant_id=?",message,maxAttempts,run,tenantId);
        throw new IllegalStateException("Ingestion failed after "+maxAttempts+" attempts; run was moved to DLQ",failure);
    }
    public Set<String> types(){return connectors.keySet();}
    private static int parseBounded(String value,int fallback,int min,int max){int n=value==null?fallback:Integer.parseInt(value);if(n<min||n>max)throw new IllegalArgumentException("Value must be between "+min+" and "+max);return n;}
    private static long parseBoundedLong(String value,long fallback,long min,long max){long n=value==null?fallback:Long.parseLong(value);if(n<min||n>max)throw new IllegalArgumentException("Value must be between "+min+" and "+max);return n;}
}
