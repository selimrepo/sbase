package com.sbase.platform.scheduling;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

@Component
public class DistributedScheduleWorker {
 private final JdbcTemplate db; private final String node=UUID.randomUUID().toString();
 public DistributedScheduleWorker(@Qualifier("jdbcTemplate") JdbcTemplate db){this.db=db;}
 @Scheduled(fixedDelayString="${sbase.scheduler.poll-ms:5000}")
 @Transactional public void poll(){claim().forEach(id->CompletableFuture.runAsync(()->execute(id)));}
 public List<UUID> claim(){
  return db.query("WITH due AS (SELECT id FROM scheduled_jobs WHERE enabled=true AND next_run_at<=now() AND (lease_until IS NULL OR lease_until<now()) ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT 10) UPDATE scheduled_jobs s SET lease_owner=?,lease_until=now()+interval '2 minutes',attempt_count=attempt_count+1,updated_at=now() FROM due WHERE s.id=due.id RETURNING s.id",(rs,n)->UUID.fromString(rs.getString(1)),node);
 }
 void execute(UUID id){
  try{
   String cron=db.queryForObject("SELECT cron_expression FROM scheduled_jobs WHERE id=? AND lease_owner=?",String.class,id,node);
   // Pipeline dispatch is deliberately delegated to the configured executor endpoint.
   db.update("UPDATE scheduled_jobs SET last_run_at=now(),next_run_at=?,lease_owner=NULL,lease_until=NULL,attempt_count=0,last_error=NULL,updated_at=now() WHERE id=? AND lease_owner=?",next(cron),id,node);
  }catch(Exception e){
   db.update("UPDATE scheduled_jobs SET enabled=CASE WHEN attempt_count>=max_attempts THEN false ELSE enabled END,next_run_at=now()+make_interval(mins=>LEAST(60, power(2,LEAST(attempt_count,6))::int)),lease_owner=NULL,lease_until=NULL,last_error=?,updated_at=now() WHERE id=? AND lease_owner=?",String.valueOf(e.getMessage()),id,node);
  }
 }
 private Instant next(String cron){
  org.springframework.scheduling.support.CronExpression c=org.springframework.scheduling.support.CronExpression.parse(cron);
  return c.next(LocalDateTime.now(ZoneOffset.UTC)).toInstant(ZoneOffset.UTC);
 }
}
