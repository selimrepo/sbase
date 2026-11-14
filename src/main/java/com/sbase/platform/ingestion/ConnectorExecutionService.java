package com.sbase.platform.ingestion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.platform.IngestionConnector;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.concurrent.Executor;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

/**
 * Durable ingestion dispatcher. HTTP only validates and persists a QUEUED run;
 * bounded workers claim work with SKIP LOCKED and leases. Execution is at-least-once,
 * while connector sinks must enforce their own idempotency keys.
 */
@Service
public class ConnectorExecutionService {
    private final Map<String, IngestionConnector> connectors;
    private final JdbcTemplate db;
    private final ObjectMapper mapper;
    private final String workerId = UUID.randomUUID().toString();
    private final int batchSize;
    private final Executor workerExecutor;

    public ConnectorExecutionService(List<IngestionConnector> list,
            @Qualifier("jdbcTemplate") JdbcTemplate db, ObjectMapper mapper,
            @Value("${sbase.ingestion.worker.batch-size:4}") int batchSize,
            @Qualifier("ingestionExecutor") Executor workerExecutor) {
        this.db = db; this.mapper = mapper;
        this.batchSize = Math.max(1, Math.min(32, batchSize));
        this.workerExecutor = workerExecutor;
        Map<String, IngestionConnector> map = new HashMap<>();
        for (IngestionConnector connector : list) {
            String connectorType = connector.type().toLowerCase(Locale.ROOT);
            if (map.put(connectorType, connector) != null)
                throw new IllegalStateException("Duplicate connector type: " + connector.type());
            // Existing tenant connector records may still be labelled "doris".
            if ("postgresql".equals(connectorType)) map.putIfAbsent("doris", connector);
        }
        connectors = Map.copyOf(map);
    }

    /** Enqueue only. Never performs network or connector I/O on the HTTP thread. */
    public Map<String, Object> execute(String type, UUID connectorId, UUID datasetId, String source, String target,
            Map<String, String> options, String idempotencyKey, UUID tenantId, String subject) {
        String normalized = type.toLowerCase(Locale.ROOT);
        if (!connectors.containsKey(normalized)) throw new IllegalArgumentException("Unsupported connector: " + type);
        if (connectorId == null) throw new IllegalArgumentException("connectorId is required");
        String registered = db.query("SELECT connector_type FROM ingestion_connectors WHERE id=? AND tenant_id=? AND enabled=true",
                rs -> rs.next() ? rs.getString(1) : null, connectorId, tenantId);
        if (registered == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Connector not found");
        boolean legacyDorisAlias = ("postgresql".equalsIgnoreCase(type) && "doris".equalsIgnoreCase(registered))
                || ("doris".equalsIgnoreCase(type) && "postgresql".equalsIgnoreCase(registered));
        if (!registered.equalsIgnoreCase(type) && !legacyDorisAlias)
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Connector type mismatch");
        // Preserve the persisted connector type in the durable request so the
        // ownership re-check remains valid for existing Doris-labelled records.
        normalized = registered.toLowerCase(Locale.ROOT);
        if (datasetId != null && !Boolean.TRUE.equals(db.query(
                "SELECT EXISTS(SELECT 1 FROM datasets WHERE id=? AND tenant_id=?)",
                rs -> rs.next() && rs.getBoolean(1), datasetId, tenantId)))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Dataset not found");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", normalized); payload.put("connectorId", connectorId.toString());
        payload.put("datasetId", datasetId == null ? null : datasetId.toString());
        payload.put("source", source); payload.put("target", target);
        payload.put("options", options == null ? Map.of() : options);
        String json;
        try { json = mapper.writeValueAsString(payload); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid ingestion request", e); }

        UUID runId = UUID.randomUUID();
        String key = idempotencyKey == null || idempotencyKey.isBlank() ? null : idempotencyKey.trim();
        int inserted = db.update("""
            INSERT INTO ingestion_runs(id,connector_id,dataset_id,tenant_id,submitted_by,status,
                submitted_at,idempotency_key,request_payload)
            VALUES(?,?,?,?,?,'QUEUED',now(),?,?::jsonb)
            ON CONFLICT (tenant_id,idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            """, runId, connectorId, datasetId, tenantId, subject, key, json);
        if (inserted == 0) {
            Map<String, Object> existing = db.queryForMap(
                "SELECT id,status,records_read,records_written FROM ingestion_runs WHERE tenant_id=? AND idempotency_key=?",
                tenantId, key);
            return Map.of("runId", existing.get("id"), "status", existing.get("status"),
                "recordsRead", Objects.toString(existing.get("records_read"), "0"),
                "recordsWritten", Objects.toString(existing.get("records_written"), "0"), "replayed", true);
        }
        return Map.of("runId", runId, "status", "QUEUED", "accepted", true, "replayed", false);
    }

    /** Multiple nodes may poll concurrently; row locks and fencing tokens elect one owner. */
    @Scheduled(fixedDelayString = "${sbase.ingestion.worker.poll-ms:500}")
    public void poll() {
        for (int i = 0; i < batchSize; i++) {
            ClaimedRun run = claimOne();
            if (run == null) return;
            try { workerExecutor.execute(() -> process(run)); }
            catch (RuntimeException rejected) {
                db.update("UPDATE ingestion_runs SET status='QUEUED',lease_owner=NULL,lease_until=NULL,error_message=? WHERE id=? AND status='RUNNING' AND lease_owner=? AND fencing_token=?",
                    "Worker executor rejected run: " + rejected.getMessage(), run.id(), workerId, run.fencingToken());
                return;
            }
        }
    }

    private ClaimedRun claimOne() {
        return db.query("""
            WITH candidate AS (
                SELECT r.id FROM ingestion_runs r
                  JOIN ingestion_connectors c ON c.id=r.connector_id AND c.tenant_id=r.tenant_id
                 WHERE c.enabled=true
                   AND (r.status='QUEUED' OR (r.status='RETRY_WAIT' AND r.next_attempt_at<=now())
                        OR (r.status='RUNNING' AND r.lease_until<now()))
                 ORDER BY r.submitted_at,r.id
                 FOR UPDATE OF r SKIP LOCKED LIMIT 1
            )
            UPDATE ingestion_runs r
               SET status='RUNNING', started_at=COALESCE(r.started_at,now()),
                   attempt_count=r.attempt_count+1, lease_owner=?,
                   lease_until=now()+interval '90 seconds', lease_heartbeat_at=now(),
                   fencing_token=COALESCE(r.fencing_token,0)+1, next_attempt_at=NULL
              FROM candidate c WHERE r.id=c.id
            RETURNING r.id,r.tenant_id,r.connector_id,r.request_payload,r.fencing_token,r.attempt_count
            """, rs -> rs.next() ? new ClaimedRun(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
                rs.getObject(3, UUID.class), rs.getString(4), rs.getLong(5), rs.getInt(6)) : null, workerId);
    }

    @Scheduled(fixedDelayString = "${sbase.ingestion.worker.heartbeat-ms:20000}")
    public void heartbeatLeases() {
        db.update("""
            UPDATE ingestion_runs SET lease_until=now()+interval '90 seconds',lease_heartbeat_at=now()
             WHERE status='RUNNING' AND lease_owner=? AND lease_until>now()
            """, workerId);
    }

    private void process(ClaimedRun run) {
        RuntimeException failure = null;
        try {
            Map<String, Object> payload = mapper.readValue(run.payload(), new TypeReference<>() {});
            String type = (String) payload.get("type");
            Boolean connectorOwned=db.query("""
                SELECT EXISTS(SELECT 1 FROM ingestion_runs r
                    JOIN ingestion_connectors c ON c.id=r.connector_id AND c.tenant_id=r.tenant_id
                 WHERE r.id=? AND r.tenant_id=? AND r.connector_id=? AND c.enabled=true
                   AND LOWER(c.connector_type)=LOWER(?))
                """,rs->rs.next()&&rs.getBoolean(1),run.id(),run.tenantId(),run.connectorId(),type);
            if(!Boolean.TRUE.equals(connectorOwned))
                throw new SecurityException("Ingestion run connector ownership or tenant binding is invalid");
            Map<String, String> options = mapper.convertValue(payload.get("options"), new TypeReference<>() {});
            options = new HashMap<>(options == null ? Map.of() : options);
            options.put("connectorId", run.connectorId().toString());
            options.put("tenantId", run.tenantId().toString());
            IngestionConnector connector = connectors.get(type);
            var result = connector.ingest(new IngestionConnector.ConnectorRequest(
                Objects.toString(payload.get("source"), ""), Objects.toString(payload.get("target"), ""), Map.copyOf(options)));
            db.update("""
                UPDATE ingestion_runs SET status='SUCCEEDED',finished_at=now(),records_read=?,
                    records_written=?,lease_owner=NULL,lease_until=NULL
                 WHERE id=? AND tenant_id=? AND status='RUNNING' AND lease_owner=? AND fencing_token=? AND lease_until>now()
                """, result.recordsRead(), result.recordsWritten(), run.id(), run.tenantId(), workerId, run.fencingToken());
            return;
        } catch (Exception ex) {
            failure = ex instanceof RuntimeException re ? re : new IllegalStateException(ex);
        }
        String message = failure == null ? "Unknown ingestion failure" : Objects.toString(failure.getMessage(), failure.getClass().getSimpleName());
        boolean retry = run.attempt() < 5;
        try {
            if (!retry) {
                db.update("""
                    INSERT INTO ingestion_dead_letters(id,run_id,connector_id,tenant_id,connector_type,
                        source_ref,payload,error_message,attempt_count,created_at)
                    SELECT gen_random_uuid(),id,connector_id,tenant_id,
                        COALESCE(request_payload->>'type','unknown'),request_payload->>'source',
                        request_payload,?, ?,now()
                    FROM ingestion_runs WHERE id=? AND tenant_id=? AND status='RUNNING'
                      AND lease_owner=? AND fencing_token=? AND lease_until>now()
                    """, message, run.attempt(), run.id(), run.tenantId(), workerId, run.fencingToken());
            }
            db.update("""
                UPDATE ingestion_runs SET status=?,next_attempt_at=CASE WHEN ? THEN
                    now()+make_interval(secs=>LEAST(900,5*power(2,LEAST(7,?))::int)) ELSE NULL END,
                    error_message=?,finished_at=CASE WHEN ? THEN NULL ELSE now() END,
                    lease_owner=NULL,lease_until=NULL
                 WHERE id=? AND tenant_id=? AND status='RUNNING' AND lease_owner=? AND fencing_token=? AND lease_until>now()
                """, retry ? "RETRY_WAIT" : "FAILED", retry, run.attempt(), message, retry,
                    run.id(), run.tenantId(), workerId, run.fencingToken());
        } catch (Exception persistenceFailure) {
            if (failure != null) failure.addSuppressed(persistenceFailure);
            // Lease expiry allows another node to recover this run; do not acknowledge it.
        }
    }

    public Set<String> types() { return connectors.values().stream().map(c -> c.type().toLowerCase(Locale.ROOT)).collect(java.util.stream.Collectors.toUnmodifiableSet()); }
    private record ClaimedRun(UUID id, UUID tenantId, UUID connectorId, String payload, long fencingToken, int attempt) { }
}
