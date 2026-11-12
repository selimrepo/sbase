package com.sbase.platform.scheduling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.pipeline.PipelineExecutionService;
import com.sbase.pipeline.PipelineRunEntity;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * PostgreSQL-backed scheduler. A due row is claimed with SKIP LOCKED and a lease,
 * then submitted after the claim transaction commits. Schedule outcomes are
 * reconciled from durable pipeline_runs records, so process restarts do not lose
 * success/failure state.
 */
@Component
public class DistributedScheduleWorker {
    private static final int CLAIM_BATCH = 10;
    private static final Duration LEASE = Duration.ofMinutes(2);

    private final JdbcTemplate db;
    private final Executor executor;
    private final PipelineExecutionService pipelines;
    private final ObjectMapper mapper;
    private final String node = UUID.randomUUID().toString();

    public DistributedScheduleWorker(@Qualifier("jdbcTemplate") JdbcTemplate db,
                                     @Qualifier("miningExecutor") Executor executor,
                                     PipelineExecutionService pipelines,
                                     ObjectMapper mapper) {
        this.db = db;
        this.executor = executor;
        this.pipelines = pipelines;
        this.mapper = mapper;
    }

    @Scheduled(fixedDelayString = "${sbase.scheduler.poll-ms:5000}")
    @Transactional
    public void poll() {
        reconcileSubmittedRuns();
        handleCompletedRuns();
        List<UUID> claimed = claim();
        if (claimed.isEmpty()) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                for (UUID id : claimed) {
                    try {
                        executor.execute(() -> execute(id));
                    } catch (RuntimeException ex) {
                        recordSubmissionFailure(id, "Scheduler executor rejected job: " + message(ex));
                    }
                }
            }
        });
    }

    List<UUID> claim() {
        return db.query("""
            WITH due AS (
                SELECT s.id
                FROM scheduled_jobs s
                JOIN pipelines p ON p.id = s.pipeline_id AND p.tenant_id = s.tenant_id
                WHERE s.enabled = TRUE
                  AND s.next_run_at <= now()
                  AND (s.lease_until IS NULL OR s.lease_until < now())
                  AND NOT EXISTS (SELECT 1 FROM schedule_runs active WHERE active.schedule_id=s.id AND active.status='SUBMITTED')
                  AND (
                    s.depends_on IS NULL OR EXISTS (
                      SELECT 1 FROM scheduled_jobs parent
                      WHERE parent.id = s.depends_on
                        AND parent.tenant_id = s.tenant_id
                        AND parent.last_success_at IS NOT NULL
                        AND parent.last_success_at >= COALESCE(s.last_run_at, s.created_at)
                    )
                  )
                ORDER BY s.next_run_at, s.created_at
                LIMIT ?
                FOR UPDATE OF s SKIP LOCKED
            )
            UPDATE scheduled_jobs s
               SET lease_owner = ?, lease_until = now() + (? * interval '1 second'),
                   attempt_count = attempt_count + 1, updated_at = now()
              FROM due
             WHERE s.id = due.id
            RETURNING s.id
            """, (rs, row) -> rs.getObject(1, UUID.class),
            CLAIM_BATCH, node, LEASE.toSeconds());
    }

    void execute(UUID id) {
        try {
            ScheduleRow schedule = db.query("""
                SELECT id, name, created_by, pipeline_id, cron_expression, timezone, calendar_json,
                       failure_policy, misfire_policy, next_run_at, attempt_count,
                       max_attempts, misfire_grace_seconds
                  FROM scheduled_jobs
                 WHERE id = ? AND lease_owner = ?
                """, rs -> rs.next() ? new ScheduleRow(
                    rs.getObject("id", UUID.class),
                    rs.getString("name"),
                    rs.getString("created_by"),
                    rs.getObject("pipeline_id", UUID.class),
                    rs.getString("cron_expression"),
                    rs.getString("timezone"),
                    rs.getString("calendar_json"),
                    rs.getString("failure_policy"),
                    rs.getString("misfire_policy"),
                    rs.getTimestamp("next_run_at").toInstant(),
                    rs.getInt("attempt_count"),
                    rs.getInt("max_attempts"),
                    rs.getInt("misfire_grace_seconds")) : null, id, node);

            if (schedule == null) return;
            if (schedule.pipelineId() == null) throw new IllegalStateException("Schedule has no pipeline configured");

            ZoneId zone = ZoneId.of(schedule.timezone() == null ? "UTC" : schedule.timezone());
            CronExpression cron = CronExpression.parse(schedule.cronExpression());
            Instant now = Instant.now();
            Instant scheduledFor = schedule.scheduledFor();
            boolean late = now.isAfter(scheduledFor.plusSeconds(Math.max(0, schedule.misfireGraceSeconds())));

            if ("SKIP".equalsIgnoreCase(schedule.misfirePolicy()) && late) {
                recordSkipped(schedule, "MISFIRE_SKIPPED");
                advance(schedule.id(), cron, ZonedDateTime.ofInstant(now, zone), zone, "MISFIRE_SKIPPED");
                return;
            }

            ZonedDateTime dueInZone = scheduledFor.atZone(zone);
            if (calendarBlocks(schedule.calendarJson(), dueInZone.toLocalDate())) {
                recordSkipped(schedule, "CALENDAR_SKIPPED");
                advance(schedule.id(), cron, ZonedDateTime.ofInstant(now, zone), zone, "CALENDAR_SKIPPED");
                return;
            }

            // RUN_ONCE coalesces missed occurrences; CATCH_UP keeps the original
            // scheduled instant and will claim the next overdue instant on a later poll.
            PipelineRunEntity run = pipelines.submit(schedule.pipelineId(), schedule.createdBy());
            db.update("""
                INSERT INTO schedule_runs
                    (id, schedule_id, pipeline_run_id, status, scheduled_for, started_at, attempt)
                VALUES (?, ?, ?, 'SUBMITTED', ?, now(), ?)
                """, UUID.randomUUID(), schedule.id(), run.getId(), scheduledFor, schedule.attemptCount());

            ZonedDateTime nextBase = "CATCH_UP".equalsIgnoreCase(schedule.misfirePolicy())
                ? dueInZone : ZonedDateTime.now(zone);
            if (!"CATCH_UP".equalsIgnoreCase(schedule.misfirePolicy()) && late) {
                nextBase = ZonedDateTime.now(zone);
            }
            advance(schedule.id(), cron, nextBase, zone, null);
        } catch (Exception ex) {
            recordSubmissionFailure(id, message(ex));
        }
    }

    private void reconcileSubmittedRuns() {
        db.update("""
            UPDATE schedule_runs sr
               SET status = pr.status,
                   finished_at = COALESCE(pr.finished_at, now()),
                   error_message = pr.error_message,
                   outcome_recorded_at = now()
              FROM pipeline_runs pr
             WHERE sr.pipeline_run_id = pr.id
               AND sr.tenant_id = pr.tenant_id
               AND sr.status = 'SUBMITTED'
               AND sr.outcome_recorded_at IS NULL
               AND pr.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
            """);
    }

    private void handleCompletedRuns() {
        List<CompletedRun> completed = db.query("""
            SELECT sr.id AS run_id, sr.schedule_id, sr.status, sr.finished_at,
                   s.cron_expression, s.timezone, s.failure_policy, s.max_attempts,
                   sr.attempt, sr.error_message
              FROM schedule_runs sr
              JOIN scheduled_jobs s ON s.id = sr.schedule_id AND s.tenant_id = sr.tenant_id
             WHERE sr.outcome_recorded_at IS NOT NULL
               AND sr.handled_at IS NULL
               AND sr.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
             ORDER BY sr.finished_at
             LIMIT 100
            """, (rs, n) -> new CompletedRun(
                rs.getObject("run_id", UUID.class),
                rs.getObject("schedule_id", UUID.class),
                rs.getString("status"),
                rs.getTimestamp("finished_at").toInstant(),
                rs.getString("cron_expression"),
                rs.getString("timezone"),
                rs.getString("failure_policy"),
                rs.getInt("max_attempts"),
                rs.getInt("attempt"), rs.getString("error_message")));

        for (CompletedRun run : completed) {
            if ("SUCCEEDED".equals(run.status())) {
                db.update("""
                    UPDATE scheduled_jobs
                       SET last_success_at = ?, last_run_at = ?, consecutive_failures = 0,
                           attempt_count = 0, last_error = NULL, updated_at = now()
                     WHERE id = ?
                    """, run.finishedAt(), run.finishedAt(), run.scheduleId());
            } else {
                applyFailurePolicy(run);
            }
            db.update("UPDATE schedule_runs SET handled_at = now() WHERE id = ? AND handled_at IS NULL", run.runId());
        }
    }

    private void applyFailurePolicy(CompletedRun run) {
        String policy = run.failurePolicy() == null ? "RETRY" : run.failurePolicy().toUpperCase(Locale.ROOT);
        if ("RETRY".equals(policy) && run.attempt() < run.maxAttempts()) {
            long delaySeconds = Math.min(3600L, 10L * (1L << Math.min(8, Math.max(0, run.attempt() - 1))));
            db.update("""
                UPDATE scheduled_jobs
                   SET consecutive_failures = consecutive_failures + 1,
                       next_run_at = now() + (? * interval '1 second'),
                       attempt_count = ?, last_error = ?,
                       updated_at = now()
                 WHERE id = ?
                """, delaySeconds, run.errorMessage(), run.attempt(), run.scheduleId());
            return;
        }

        ScheduleRow schedule = db.query("""
            SELECT id, name, created_by, pipeline_id, cron_expression, timezone, calendar_json,
                   failure_policy, misfire_policy, next_run_at, attempt_count,
                   max_attempts, misfire_grace_seconds
              FROM scheduled_jobs WHERE id = ?
            """, rs -> rs.next() ? new ScheduleRow(
                rs.getObject("id", UUID.class), rs.getString("name"),
                rs.getString("created_by"),
                rs.getObject("pipeline_id", UUID.class), rs.getString("cron_expression"),
                rs.getString("timezone"), rs.getString("calendar_json"),
                rs.getString("failure_policy"), rs.getString("misfire_policy"),
                rs.getTimestamp("next_run_at").toInstant(), rs.getInt("attempt_count"),
                rs.getInt("max_attempts"), rs.getInt("misfire_grace_seconds")) : null, run.scheduleId());

        boolean disable = "DISABLE".equals(policy);
        if (schedule != null) {
            ZoneId zone = ZoneId.of(schedule.timezone() == null ? "UTC" : schedule.timezone());
            ZonedDateTime next = nextOccurrence(CronExpression.parse(schedule.cronExpression()), ZonedDateTime.now(zone), zone);
            db.update("""
                UPDATE scheduled_jobs
                   SET enabled = CASE WHEN ? THEN FALSE ELSE enabled END,
                       consecutive_failures = consecutive_failures + 1,
                       last_run_at = now(), next_run_at = ?, attempt_count = 0,
                       last_error = ?, updated_at = now()
                 WHERE id = ?
                """, disable, next.toInstant(), run.errorMessage(), run.scheduleId());
        }
    }

    private void recordSkipped(ScheduleRow schedule, String reason) {
        db.update("""
            INSERT INTO schedule_runs(id, schedule_id, status, scheduled_for, started_at, finished_at,
                                      attempt, error_message, outcome_recorded_at, handled_at)
            VALUES (?, ?, ?, ?, now(), now(), ?, ?, now(), now())
            """, UUID.randomUUID(), schedule.id(), reason, schedule.scheduledFor(),
            schedule.attemptCount(), reason);
    }

    private void advance(UUID id, CronExpression cron, ZonedDateTime base, ZoneId zone, String reason) {
        ZonedDateTime next = nextOccurrence(cron, base, zone);
        db.update("""
            UPDATE scheduled_jobs
               SET last_run_at = now(), next_run_at = ?, lease_owner = NULL, lease_until = NULL,
                   attempt_count = 0, last_error = ?, updated_at = now()
             WHERE id = ? AND lease_owner = ?
            """, next.toInstant(), reason, id, node);
    }

    private ZonedDateTime nextOccurrence(CronExpression cron, ZonedDateTime base, ZoneId zone) {
        ZonedDateTime next = cron.next(base.withZoneSameInstant(zone));
        if (next == null) throw new IllegalStateException("Cron expression has no next execution time");
        return next;
    }

    private boolean calendarBlocks(String calendarJson, LocalDate date) {
        return ScheduleCalendarPolicy.blocks(mapper, calendarJson, date);
    }

    private void recordSubmissionFailure(UUID id, String error) {
        ScheduleRow schedule = db.query("""
            SELECT id, name, created_by, pipeline_id, cron_expression, timezone, calendar_json,
                   failure_policy, misfire_policy, next_run_at, attempt_count,
                   max_attempts, misfire_grace_seconds
              FROM scheduled_jobs WHERE id = ? AND lease_owner = ?
            """, rs -> rs.next() ? new ScheduleRow(
                rs.getObject("id", UUID.class), rs.getString("name"), rs.getString("created_by"),
                rs.getObject("pipeline_id", UUID.class), rs.getString("cron_expression"),
                rs.getString("timezone"), rs.getString("calendar_json"),
                rs.getString("failure_policy"), rs.getString("misfire_policy"),
                rs.getTimestamp("next_run_at").toInstant(), rs.getInt("attempt_count"),
                rs.getInt("max_attempts"), rs.getInt("misfire_grace_seconds")) : null, id, node);
        if (schedule == null) return;

        db.update("""
            INSERT INTO schedule_runs(id, schedule_id, status, scheduled_for, started_at, finished_at,
                                      attempt, error_message, outcome_recorded_at, handled_at)
            VALUES (?, ?, 'FAILED', ?, now(), now(), ?, ?, now(), now())
            """, UUID.randomUUID(), id, schedule.scheduledFor(), schedule.attemptCount(), error);

        if ("RETRY".equalsIgnoreCase(schedule.failurePolicy()) && schedule.attemptCount() < schedule.maxAttempts()) {
            long delay = Math.min(3600L, 10L * (1L << Math.min(8, Math.max(0, schedule.attemptCount() - 1))));
            db.update("""
                UPDATE scheduled_jobs SET next_run_at=now()+(? * interval '1 second'),
                    consecutive_failures=consecutive_failures+1, last_error=?,
                    lease_owner=NULL, lease_until=NULL, updated_at=now()
                WHERE id=? AND lease_owner=?
                """, delay, error, id, node);
        } else {
            boolean disable = "DISABLE".equalsIgnoreCase(schedule.failurePolicy())
                || schedule.attemptCount() >= schedule.maxAttempts();
            ZoneId zone = ZoneId.of(schedule.timezone() == null ? "UTC" : schedule.timezone());
            ZonedDateTime next = nextOccurrence(CronExpression.parse(schedule.cronExpression()), ZonedDateTime.now(zone), zone);
            db.update("""
                UPDATE scheduled_jobs SET enabled=CASE WHEN ? THEN FALSE ELSE enabled END,
                    next_run_at=?, consecutive_failures=consecutive_failures+1, attempt_count=0,
                    last_error=?, lease_owner=NULL, lease_until=NULL, updated_at=now()
                WHERE id=? AND lease_owner=?
                """, disable, next.toInstant(), error, id, node);
        }
    }

    private static String message(Throwable error) {
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }

    private record ScheduleRow(UUID id, String name, String createdBy, UUID pipelineId, String cronExpression,
                               String timezone, String calendarJson, String failurePolicy,
                               String misfirePolicy, Instant scheduledFor, int attemptCount,
                               int maxAttempts, int misfireGraceSeconds) { }

    private record CompletedRun(UUID runId, UUID scheduleId, String status, Instant finishedAt,
                                String cronExpression, String timezone, String failurePolicy,
                                int maxAttempts, int attempt, String errorMessage) { }
}
