package com.sbase.platform.scheduling;

import com.sbase.pipeline.PipelineRunEntity;
import com.sbase.pipeline.PipelineRunService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

@Component
public class DistributedScheduleWorker {
    private final JdbcTemplate db;
    private final Executor executor;
    private final PipelineRunService pipelineRuns;
    private final String node=UUID.randomUUID().toString();

    public DistributedScheduleWorker(@Qualifier("jdbcTemplate") JdbcTemplate db,
                                     @Qualifier("miningExecutor") Executor executor,
                                     PipelineRunService pipelineRuns){
        this.db=db;this.executor=executor;this.pipelineRuns=pipelineRuns;
    }

    @Scheduled(fixedDelayString="${sbase.scheduler.poll-ms:5000}")
    @Transactional
    public void poll(){
        // Keep leases alive while a Spark action is running; a long job must not be claimed twice.
        db.update("UPDATE scheduled_jobs SET lease_until=now()+interval '2 minutes',updated_at=now() WHERE lease_owner=? AND lease_until>now()",node);
        List<ClaimedSchedule> claimed=claim();
        if(claimed.isEmpty())return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){
            @Override public void afterCommit(){
                claimed.forEach(schedule->{
                    try{executor.execute(()->execute(schedule));}
                    catch(RuntimeException ex){recordFailure(schedule.id(),null,"Schedule executor rejected the job: "+ex.getMessage());}
                });
            }
        });
    }

    List<ClaimedSchedule> claim(){
        return db.query("""
            WITH due AS (
                SELECT id FROM scheduled_jobs
                WHERE enabled=true AND next_run_at<=now()
                  AND (lease_until IS NULL OR lease_until<now())
                ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT 10
            )
            UPDATE scheduled_jobs s
            SET lease_owner=?,lease_until=now()+interval '2 minutes',
                attempt_count=attempt_count+1,updated_at=now()
            FROM due WHERE s.id=due.id
            RETURNING s.id,s.pipeline_id,s.cron_expression
            """,(rs,n)->new ClaimedSchedule(UUID.fromString(rs.getString("id")),
                rs.getObject("pipeline_id",UUID.class),rs.getString("cron_expression")),node);
    }

    void execute(ClaimedSchedule schedule){
        try{
            if(schedule.pipelineId()==null)throw new IllegalStateException("Schedule has no pipeline assigned");
            PipelineRunEntity run=pipelineRuns.executeScheduled(schedule.pipelineId(),schedule.id());
            if(!"SUCCEEDED".equals(run.getStatus())){
                recordFailure(schedule.id(),run.getId(),run.getErrorMessage()==null?"Pipeline run failed":run.getErrorMessage());
                return;
            }
            LocalDateTime next=CronExpression.parse(schedule.cronExpression()).next(LocalDateTime.now(ZoneOffset.UTC));
            if(next==null)throw new IllegalStateException("Schedule cron has no next execution time");
            db.update("""
                UPDATE scheduled_jobs SET next_run_at=?,last_run_at=now(),last_run_id=?,last_status='SUCCEEDED',
                    last_error=NULL,attempt_count=0,lease_owner=NULL,lease_until=NULL,updated_at=now()
                WHERE id=? AND lease_owner=?
                """,next.toInstant(ZoneOffset.UTC),run.getId(),schedule.id(),node);
        }catch(Exception ex){
            recordFailure(schedule.id(),null,ex.getMessage()==null?ex.getClass().getSimpleName():ex.getMessage());
        }
    }

    private void recordFailure(UUID id,UUID runId,String message){
        db.update("""
            UPDATE scheduled_jobs
            SET enabled=CASE WHEN attempt_count>=max_attempts THEN false ELSE enabled END,
                next_run_at=now()+make_interval(mins=>LEAST(60,power(2,LEAST(attempt_count,6))::int)),
                last_run_at=now(),last_run_id=COALESCE(?,last_run_id),last_status='FAILED',
                lease_owner=NULL,lease_until=NULL,last_error=?,updated_at=now()
            WHERE id=? AND lease_owner=?
            """,runId,message,id,node);
    }

    record ClaimedSchedule(UUID id,UUID pipelineId,String cronExpression){}
}
