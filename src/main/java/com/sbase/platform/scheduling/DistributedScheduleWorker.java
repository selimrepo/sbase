package com.sbase.platform.scheduling;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

@Component
public class DistributedScheduleWorker {
    private final JdbcTemplate db;
    private final Executor executor;
    private final String node = UUID.randomUUID().toString();

    public DistributedScheduleWorker(@Qualifier("jdbcTemplate") JdbcTemplate db,
                                     @Qualifier("miningExecutor") Executor executor) {
        this.db = db;
        this.executor = executor;
    }

    @Scheduled(fixedDelayString = "${sbase.scheduler.poll-ms:5000}")
    @Transactional
    public void poll() {
        List<UUID> claimed = claim();
        if (claimed.isEmpty()) return;

        // Do not let workers race the transaction that writes their leases.
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                claimed.forEach(id -> {
                    try {
                        executor.execute(() -> execute(id));
                    } catch (RuntimeException ex) {
                        recordFailure(id, "Schedule executor rejected the job: " + ex.getMessage());
                    }
                });
            }
        });
    }

    public List<UUID> claim() {
        return db.query("""
                WITH due AS (
                    SELECT id FROM scheduled_jobs
                    WHERE enabled=true AND next_run_at<=now()
                      AND (lease_until IS NULL OR lease_until<now())
                    ORDER BY next_run_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 10
                )
                UPDATE scheduled_jobs s
                SET lease_owner=?, lease_until=now()+interval '2 minutes',
                    attempt_count=attempt_count+1, updated_at=now()
                FROM due WHERE s.id=due.id
                RETURNING s.id
                """, (rs, n) -> UUID.fromString(rs.getString(1)), node);
    }

    void execute(UUID id) {
        try {
            // Pipeline definitions are metadata only: there is no pipeline execution
            // engine/step registry yet. Never mark a schedule successful as a no-op.
            throw new IllegalStateException(
                    "Pipeline execution is not configured; schedule cannot be marked successful");
        } catch (Exception ex) {
            recordFailure(id, String.valueOf(ex.getMessage()));
        }
    }

    private void recordFailure(UUID id, String message) {
        db.update("""
                UPDATE scheduled_jobs
                SET enabled=CASE WHEN attempt_count>=max_attempts THEN false ELSE enabled END,
                    next_run_at=now()+make_interval(mins=>LEAST(60, power(2,LEAST(attempt_count,6))::int)),
                    lease_owner=NULL, lease_until=NULL, last_error=?, updated_at=now()
                WHERE id=? AND lease_owner=?
                """, message, id, node);
    }
}
