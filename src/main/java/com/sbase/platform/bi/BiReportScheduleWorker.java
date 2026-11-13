package com.sbase.platform.bi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;

@Component
public class BiReportScheduleWorker {
    private static final Pattern IDENT = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");
    private final JdbcTemplate db;
    private final DorisBiQueryService bi;
    private final ObjectMapper mapper;
    private final Executor executor;
    private final String node = UUID.randomUUID().toString();

    public BiReportScheduleWorker(@Qualifier("jdbcTemplate") JdbcTemplate db,
                                  DorisBiQueryService bi,
                                  ObjectMapper mapper,
                                  @Qualifier("miningExecutor") Executor executor) {
        this.db = db;
        this.bi = bi;
        this.mapper = mapper;
        this.executor = executor;
    }

    @Scheduled(fixedDelayString = "${sbase.bi.scheduler.poll-ms:15000}")
    @Transactional
    public void poll() {
        List<UUID> ids = db.query("""
            WITH due AS (
                SELECT id FROM bi_report_schedules
                 WHERE enabled=TRUE AND next_run_at<=now()
                   AND (lease_until IS NULL OR lease_until<now())
                   AND NOT EXISTS (
                       SELECT 1 FROM bi_report_schedule_runs r
                        WHERE r.schedule_id=bi_report_schedules.id AND r.status='RUNNING'
                   )
                 ORDER BY next_run_at
                 LIMIT 10
                 FOR UPDATE SKIP LOCKED
            )
            UPDATE bi_report_schedules s
               SET lease_owner=?,lease_until=now()+interval '2 minutes',updated_at=now()
              FROM due WHERE s.id=due.id
            RETURNING s.id
            """, (rs, row) -> rs.getObject(1, UUID.class), node);
        if (ids.isEmpty()) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                for (UUID id : ids) {
                    try { executor.execute(() -> execute(id)); }
                    catch (RuntimeException ex) { fail(id, ex.getMessage()); }
                }
            }
        });
    }

    void execute(UUID scheduleId) {
        Schedule schedule = db.query("""
            SELECT s.id,s.tenant_id,s.report_id,s.owner_subject,s.cron_expression,s.timezone,r.definition_json
              FROM bi_report_schedules s
              JOIN bi_reports r ON r.id=s.report_id AND r.tenant_id=s.tenant_id AND r.owner_subject=s.owner_subject
             WHERE s.id=? AND s.lease_owner=?
            """, rs -> rs.next() ? new Schedule(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("report_id", UUID.class), rs.getString("owner_subject"),
                rs.getString("cron_expression"), rs.getString("timezone"),
                rs.getString("definition_json")) : null, scheduleId, node);
        if (schedule == null) return;
        UUID runId = UUID.randomUUID();
        db.update("INSERT INTO bi_report_schedule_runs(id,schedule_id,tenant_id,status,started_at) VALUES(?,?,?,'RUNNING',now())", runId, scheduleId, schedule.tenantId());
        try {
            List<Map<String,Object>> rows = runQuery(schedule.tenantId(), schedule.definitionJson());
            String result = mapper.writeValueAsString(Map.of("rows", rows, "rowCount", rows.size()));
            db.update("""
                UPDATE bi_report_schedule_runs SET status='SUCCEEDED',row_count=?,result_json=?,finished_at=now()
                 WHERE id=? AND tenant_id=?
                """, rows.size(), result, runId, schedule.tenantId());
            advance(schedule, true, null);
        } catch (Exception ex) {
            String message = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
            db.update("""
                UPDATE bi_report_schedule_runs SET status='FAILED',error_message=?,finished_at=now()
                 WHERE id=? AND tenant_id=?
                """, message, runId, schedule.tenantId());
            fail(scheduleId, message);
        }
    }

    private List<Map<String,Object>> runQuery(UUID tenantId, String definitionJson) throws Exception {
        JsonNode d = mapper.readTree(definitionJson);
        String table = identifier(d.path("table").asText());
        JsonNode dimensions = d.path("dimensions");
        JsonNode metrics = d.path("metrics");
        if (!dimensions.isArray() || dimensions.size()>20 || !metrics.isArray() || metrics.isEmpty() || metrics.size()>20)
            throw new IllegalArgumentException("Invalid report dimensions or metrics");
        List<String> select = new ArrayList<>();
        for (JsonNode dimension : dimensions) select.add(identifier(dimension.asText()));
        for (JsonNode metric : metrics) {
            String column = identifier(metric.path("column").asText());
            String alias = identifier(metric.path("alias").asText());
            String function = metric.path("function").asText().toUpperCase(Locale.ROOT);
            if (!Set.of("COUNT","SUM","AVG","MIN","MAX").contains(function))
                throw new IllegalArgumentException("Unsupported aggregate function");
            select.add(function+"("+column+") AS "+alias);
        }
        StringBuilder sql = new StringBuilder("SELECT ").append(String.join(",",select)).append(" FROM ").append(table);
        List<String> filters = new ArrayList<>();
        JsonNode filterNodes = d.path("filters");
        if (filterNodes.isArray()) {
            if (filterNodes.size()>50) throw new IllegalArgumentException("Too many report filters");
            for (JsonNode f : filterNodes) filters.add(filter(f));
        }
        if (!filters.isEmpty()) sql.append(" WHERE ").append(String.join(" AND ",filters));
        if (!dimensions.isEmpty()) sql.append(" GROUP BY ").append(String.join(",",select.subList(0,dimensions.size())));
        int limit = d.path("limit").asInt(1000);
        if (limit<1 || limit>10000) throw new IllegalArgumentException("Report limit must be 1..10000");
        sql.append(" LIMIT ").append(limit);
        return bi.query(tenantId, sql.toString());
    }

    private String filter(JsonNode f) {
        String column = identifier(f.path("column").asText());
        String op = f.path("operator").asText().toUpperCase(Locale.ROOT);
        JsonNode value = f.path("value");
        return switch (op) {
            case "IS_NULL" -> column+" IS NULL";
            case "NOT_NULL" -> column+" IS NOT NULL";
            case "IN" -> {
                if (!value.isArray() || value.isEmpty() || value.size()>100) throw new IllegalArgumentException("IN requires 1..100 values");
                List<String> values = new ArrayList<>(); value.forEach(v -> values.add(literal(v)));
                yield column+" IN ("+String.join(",",values)+")";
            }
            case "BETWEEN" -> {
                if (!value.isArray() || value.size()!=2) throw new IllegalArgumentException("BETWEEN requires two values");
                yield column+" BETWEEN "+literal(value.get(0))+" AND "+literal(value.get(1));
            }
            case "EQ","NE","GT","GTE","LT","LTE","CONTAINS","STARTS_WITH","ENDS_WITH" -> {
                String operator = switch(op){case "EQ"->"=";case "NE"->"<>";case "GT"->">";case "GTE"->">=";case "LT"->"<";case "LTE"->"<=";default->"LIKE";};
                if ("CONTAINS".equals(op) || "STARTS_WITH".equals(op) || "ENDS_WITH".equals(op)) {
                    String val=value.asText();
                    if ("CONTAINS".equals(op)) val="%"+val+"%";
                    if ("STARTS_WITH".equals(op)) val=val+"%";
                    if ("ENDS_WITH".equals(op)) val="%"+val;
                    yield column+" "+operator+" "+literalText(val);
                }
                yield column+" "+operator+" "+literal(value);
            }
            default -> throw new IllegalArgumentException("Unsupported report filter operator: "+op);
        };
    }

    private String literal(JsonNode value) {
        if (value == null || value.isNull()) return "NULL";
        if (value.isNumber()) return value.numberValue().toString();
        if (value.isBoolean()) return value.booleanValue()?"TRUE":"FALSE";
        return literalText(value.asText());
    }
    private String literalText(String value) {
        if (value.length()>2000) throw new IllegalArgumentException("Filter value too long");
        return "'"+value.replace("'","''")+"'";
    }
    private String identifier(String value) {
        if (value==null || !IDENT.matcher(value).matches()) throw new IllegalArgumentException("Unsafe report identifier");
        return value;
    }

    private void fail(UUID id, String message) {
        Schedule schedule = db.query("""
            SELECT id,tenant_id,report_id,owner_subject,cron_expression,timezone,'' AS definition_json
              FROM bi_report_schedules WHERE id=? AND lease_owner=?
            """, rs -> rs.next()?new Schedule(rs.getObject("id",UUID.class),rs.getObject("tenant_id",UUID.class),rs.getObject("report_id",UUID.class),
                rs.getString("owner_subject"),rs.getString("cron_expression"),rs.getString("timezone"),""):null,id,node);
        if (schedule == null) return;
        advance(schedule, false, message==null?"Unknown BI schedule failure":message);
    }

    private void advance(Schedule schedule, boolean success, String error) {
        try {
            ZoneId zone=ZoneId.of(schedule.timezone()==null?"UTC":schedule.timezone());
            ZonedDateTime next=CronExpression.parse(schedule.cronExpression()).next(ZonedDateTime.now(zone));
            if(next==null)throw new IllegalStateException("Cron has no next occurrence");
            db.update("""
                UPDATE bi_report_schedules
                   SET next_run_at=?,last_run_at=now(),
                       failure_count=CASE WHEN ? THEN 0 ELSE failure_count+1 END,
                       enabled=CASE WHEN ? THEN enabled ELSE failure_count+1<5 END,
                       lease_owner=NULL,lease_until=NULL,updated_at=now()
                 WHERE id=? AND tenant_id=? AND lease_owner=?
                """,next.toInstant(),success,success,schedule.id(),schedule.tenantId(),node);
        } catch (Exception ex) {
            db.update("UPDATE bi_report_schedules SET enabled=false,lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE id=? AND tenant_id=? AND lease_owner=?",schedule.id(),schedule.tenantId(),node);
        }
    }

    private record Schedule(UUID id,UUID tenantId,UUID reportId,String owner,String cronExpression,String timezone,String definitionJson) {}
}
