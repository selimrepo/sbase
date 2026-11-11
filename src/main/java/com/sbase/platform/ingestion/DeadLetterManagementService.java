package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class DeadLetterManagementService {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final ConnectorExecutionService executions;

    public DeadLetterManagementService(JdbcTemplate jdbc,ObjectMapper mapper,ConnectorExecutionService executions) {
        this.jdbc=jdbc;this.mapper=mapper;this.executions=executions;
    }

    public List<Map<String,Object>> pending(int limit) {
        int bounded=Math.max(1,Math.min(limit,200));
        return jdbc.queryForList("""
            SELECT id,run_id,connector_id,connector_type,source_ref,error_message,attempt_count,
                   replay_attempts,last_replay_error,created_at
            FROM ingestion_dead_letters
            WHERE replayed_at IS NULL AND replay_started_at IS NULL
            ORDER BY created_at ASC
            LIMIT ?
            """,bounded);
    }

    public Map<String,Object> replay(UUID id) {
        int claimed=jdbc.update("""
            UPDATE ingestion_dead_letters
            SET replay_started_at=now(),replay_attempts=replay_attempts+1,last_replay_error=NULL
            WHERE id=? AND replayed_at IS NULL
              AND (replay_started_at IS NULL OR replay_started_at < now()-interval '15 minutes')
            """,id);
        if(claimed==0) throw new IllegalStateException("Dead-letter record is missing, already replayed, or currently being replayed");

        try {
            Map<String,Object> row=jdbc.query("""
                SELECT connector_id,connector_type,payload
                FROM ingestion_dead_letters WHERE id=?
                """,rs->rs.next()?Map.of(
                    "connectorId",rs.getObject("connector_id"),
                    "type",rs.getString("connector_type"),
                    "payload",rs.getString("payload")
                ):null,id);
            if(row==null) throw new NoSuchElementException("Dead-letter record not found");
            JsonNode config=mapper.readTree(String.valueOf(row.get("payload")));
            String source=config.path("source").asText("");
            String target=config.path("target").asText("");
            Map<String,String> options=new HashMap<>();
            config.path("options").fields().forEachRemaining(e->options.put(e.getKey(),e.getValue().asText()));
            UUID connectorId=(UUID)row.get("connectorId");
            options.put("connectorId",connectorId.toString());
            Map<String,Object> result=executions.execute(String.valueOf(row.get("type")),connectorId,null,source,target,options,"dlq-replay:"+id+":"+UUID.randomUUID());
            jdbc.update("UPDATE ingestion_dead_letters SET replayed_at=now(),replay_started_at=NULL,last_replay_error=NULL WHERE id=?",id);
            return Map.of("deadLetterId",id,"replayed",true,"execution",result);
        } catch(Exception ex) {
            jdbc.update("UPDATE ingestion_dead_letters SET replay_started_at=NULL,last_replay_error=? WHERE id=?",
                String.valueOf(ex.getMessage()),id);
            if(ex instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException("Unable to replay dead-letter record",ex);
        }
    }
}
