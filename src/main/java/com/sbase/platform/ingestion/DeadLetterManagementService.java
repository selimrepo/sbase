package com.sbase.platform.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
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
            SELECT id,run_id,connector_id,connector_type,source_ref,error_message,attempt_count,created_at
            FROM ingestion_dead_letters
            WHERE replayed_at IS NULL
            ORDER BY created_at ASC
            LIMIT ?
            """,bounded);
    }

    @Transactional
    public Map<String,Object> replay(UUID deadLetterId) {
        Map<String,Object> row=jdbc.query("""
            SELECT id,connector_id,connector_type,payload,replayed_at
            FROM ingestion_dead_letters WHERE id=? FOR UPDATE
            """,rs->rs.next()?Map.of(
                "id",rs.getObject("id"),
                "connectorId",rs.getObject("connector_id"),
                "type",rs.getString("connector_type"),
                "payload",rs.getString("payload"),
                "replayedAt",rs.getTimestamp("replayed_at")==null?"":rs.getTimestamp("replayed_at").toString()
            ):null,deadLetterId);
        if(row==null) throw new NoSuchElementException("Dead-letter record not found");
        if(!String.valueOf(row.get("replayedAt")).isBlank()) throw new IllegalStateException("Dead-letter record was already replayed");
        try {
            JsonNode config=mapper.readTree(String.valueOf(row.get("payload")));
            String source=config.path("source").asText("");
            String target=config.path("target").asText("");
            Map<String,String> options=new HashMap<>();
            config.path("options").fields().forEachRemaining(e->options.put(e.getKey(),e.getValue().asText()));
            UUID connectorId=(UUID)row.get("connectorId");
            options.put("connectorId",connectorId.toString());
            String replayKey="dlq-replay:"+deadLetterId;
            Map<String,Object> result=executions.execute(String.valueOf(row.get("type")),connectorId,null,source,target,options,replayKey);
            jdbc.update("UPDATE ingestion_dead_letters SET replayed_at=now() WHERE id=? AND replayed_at IS NULL",deadLetterId);
            return Map.of("deadLetterId",deadLetterId,"replayed",true,"execution",result);
        } catch(RuntimeException ex) { throw ex; }
        catch(Exception ex) { throw new IllegalStateException("Unable to replay dead-letter record",ex); }
    }
}
