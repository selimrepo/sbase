package com.sbase.pipeline;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * PostgreSQL-backed lease/fencing primitives. Every state transition is conditional
 * on the monotonically increasing fencing token; a worker whose lease was replaced
 * cannot heartbeat or publish a terminal result.
 */
@Repository
public class PipelineLeaseCoordinator {
    private final JdbcTemplate jdbc;
    private final long leaseSeconds;

    public PipelineLeaseCoordinator(@Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            @Value("${sbase.pipeline.lease-seconds:60}") long leaseSeconds) {
        this.jdbc = jdbc;
        if (leaseSeconds < 10 || leaseSeconds > 3600) throw new IllegalArgumentException("pipeline lease must be 10..3600 seconds");
        this.leaseSeconds = leaseSeconds;
    }

    @Transactional
    public Lease claim(UUID runId, String workerId) {
        return jdbc.query("""
            UPDATE pipeline_runs
               SET status='RUNNING', started_at=COALESCE(started_at,now()), finished_at=NULL,
                   attempt_count=attempt_count+1, next_attempt_at=NULL,
                   lease_owner=?, lease_until=now()+(? * interval '1 second'),
                   lease_heartbeat_at=now(), fencing_token=fencing_token+1, error_message=NULL
             WHERE id=? AND (status='QUEUED' OR (status='RETRY_WAIT' AND next_attempt_at<=now()))
            RETURNING id, fencing_token, lease_until
            """, rs -> rs.next() ? new Lease(rs.getObject(1, UUID.class), rs.getLong(2), rs.getTimestamp(3).toInstant()) : null,
            workerId, leaseSeconds, runId);
    }

    @Transactional
    public boolean heartbeat(Lease lease, String workerId) {
        return jdbc.update("""
            UPDATE pipeline_runs
               SET lease_until=now()+(? * interval '1 second'), lease_heartbeat_at=now()
             WHERE id=? AND status='RUNNING' AND lease_owner=? AND fencing_token=? AND lease_until>now()
            """, leaseSeconds, lease.runId(), workerId, lease.fencingToken()) == 1;
    }

    @Transactional
    public boolean complete(Lease lease, String workerId, String resultJson, long recordsProcessed) {
        return jdbc.update("""
            UPDATE pipeline_runs
               SET status='SUCCEEDED', result_json=?, records_processed=?, finished_at=now(),
                   lease_until=NULL, lease_owner=NULL, lease_heartbeat_at=now()
             WHERE id=? AND status='RUNNING' AND lease_owner=? AND fencing_token=? AND lease_until>now()
            """, resultJson, recordsProcessed, lease.runId(), workerId, lease.fencingToken()) == 1;
    }

    @Transactional
    public boolean fail(Lease lease, String workerId, String message, boolean retryable) {
        return jdbc.update("""
            UPDATE pipeline_runs
               SET status=CASE WHEN ? AND attempt_count<max_attempts THEN 'RETRY_WAIT' ELSE 'FAILED' END,
                   next_attempt_at=CASE WHEN ? AND attempt_count<max_attempts
                     THEN now()+make_interval(secs=>LEAST(900,10*power(2,LEAST(GREATEST(attempt_count-1,0),7))::int))
                     ELSE NULL END,
                   error_message=?, finished_at=now(), lease_until=NULL, lease_owner=NULL
             WHERE id=? AND status='RUNNING' AND lease_owner=? AND fencing_token=?
            """, retryable, retryable, message, lease.runId(), workerId, lease.fencingToken()) == 1;
    }

    public record Lease(UUID runId, long fencingToken, java.time.Instant leaseUntil) { }
}
