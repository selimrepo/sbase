package com.sbase.platform.ingestion;
import com.sbase.platform.IngestionConnector;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.*;
@Service
public class ConnectorExecutionService {
 private final Map<String,IngestionConnector> connectors; private final JdbcTemplate db;
 public ConnectorExecutionService(List<IngestionConnector> list,JdbcTemplate db){this.db=db;Map<String,IngestionConnector> m=new HashMap<>();for(var c:list)m.put(c.type(),c);connectors=Map.copyOf(m);}
 public Map<String,Object> execute(String type,UUID connectorId,UUID datasetId,String source,String target,Map<String,String> options,String idempotencyKey){
  var connector=connectors.get(type.toLowerCase(Locale.ROOT));if(connector==null)throw new IllegalArgumentException("Unsupported connector: "+type);
  UUID run=UUID.randomUUID();Instant start=Instant.now();
  db.update("INSERT INTO ingestion_runs(id,connector_id,dataset_id,status,started_at,idempotency_key) VALUES(?,?,?,'RUNNING',?,?)",run,connectorId,datasetId,start,idempotencyKey);
  try{var result=connector.ingest(new com.sbase.platform.IngestionConnector.ConnectorRequest(source,target,options));
   db.update("UPDATE ingestion_runs SET status='SUCCEEDED',finished_at=now(),records_read=?,records_written=? WHERE id=?",result.recordsRead(),result.recordsWritten(),run);
   return Map.of("runId",run,"status","SUCCEEDED","recordsRead",result.recordsRead(),"recordsWritten",result.recordsWritten(),"details",result.details());
  }catch(RuntimeException ex){db.update("UPDATE ingestion_runs SET status='FAILED',finished_at=now(),error_message=? WHERE id=?",String.valueOf(ex.getMessage()),run);throw ex;}
 }
 public Set<String> types(){return connectors.keySet();}
}
