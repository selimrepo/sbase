package com.sbase.platform.bi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;

@Component
public class GovernedBiScheduleWorker {
 private final JdbcTemplate db;private final BiWorkspaceService bi;private final ObjectMapper mapper;private final java.util.concurrent.Executor executor;
 private final ObjectProvider<JavaMailSender> mail;private final String root;private final String node=UUID.randomUUID().toString();
 public GovernedBiScheduleWorker(@Qualifier("jdbcTemplate") JdbcTemplate db,BiWorkspaceService bi,ObjectMapper mapper,
  @Qualifier("miningExecutor") java.util.concurrent.Executor executor,ObjectProvider<JavaMailSender> mail,@Value("${sbase.bi.artifact-root:./data/bi-reports}") String root){
  this.db=db;this.bi=bi;this.mapper=mapper;this.executor=executor;this.mail=mail;this.root=root;
 }
 @Scheduled(fixedDelayString="${sbase.bi.scheduler.poll-ms:15000}")
 @Transactional
 public void poll(){
  db.update("""
   UPDATE sbase_bi_schedule_runs r SET status='FAILED',error_message='Worker lease expired',finished_at=now()
   FROM sbase_bi_schedules s
   WHERE r.schedule_id=s.id AND r.tenant_id=s.tenant_id AND r.status='RUNNING' AND s.lease_until<now()
   """);
  List<UUID> ids=db.query("""
   WITH due AS (SELECT id FROM sbase_bi_schedules WHERE enabled=true AND next_run_at<=now()
    AND (lease_until IS NULL OR lease_until<now()) ORDER BY next_run_at LIMIT 10 FOR UPDATE SKIP LOCKED)
   UPDATE sbase_bi_schedules s SET lease_owner=?,lease_until=now()+interval '30 minutes'
   FROM due WHERE s.id=due.id RETURNING s.id
   """,(rs,n)->rs.getObject(1,UUID.class),node);
  if(ids.isEmpty())return;
  TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){@Override public void afterCommit(){for(UUID id:ids){try{executor.execute(()->execute(id));}catch(RuntimeException ex){db.update("UPDATE sbase_bi_schedules SET failure_count=failure_count+1,lease_owner=NULL,lease_until=NULL WHERE id=? AND lease_owner=?",id,node);}}}});
 }
 private void execute(UUID id){
  Schedule s=db.query("""
   SELECT id,tenant_id,report_id,owner_subject,cron_expression,timezone,output_format,delivery_target
   FROM sbase_bi_schedules WHERE id=? AND lease_owner=?
   """,rs->rs.next()?new Schedule(rs.getObject("id",UUID.class),rs.getObject("tenant_id",UUID.class),rs.getObject("report_id",UUID.class),
    rs.getString("owner_subject"),rs.getString("cron_expression"),rs.getString("timezone"),rs.getString("output_format"),rs.getString("delivery_target")):null,id,node);
  if(s==null)return;UUID run=UUID.randomUUID();
  db.update("INSERT INTO sbase_bi_schedule_runs(id,schedule_id,tenant_id,status) VALUES(?,?,?,'RUNNING')",run,id,s.tenant());
  try{
   Map<String,Object> result=bi.executeReport(s.tenant(),s.owner(),s.report());
   @SuppressWarnings("unchecked") List<Map<String,Object>> rows=(List<Map<String,Object>>)result.get("rows");
   Path dir=Paths.get(root).toAbsolutePath().resolve(s.tenant().toString());Files.createDirectories(dir);
   Path file=dir.resolve(run+"."+s.format().toLowerCase(Locale.ROOT));
   if("JSON".equals(s.format()))Files.writeString(file,mapper.writeValueAsString(rows),StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);
   else Files.writeString(file,csv(rows),StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);
   String uri=file.toUri().toString();
   if(s.target()!=null&&s.target().startsWith("mailto:"))sendMail(s.target().substring(7),s,file,uri,rows.size());
   db.update("UPDATE sbase_bi_schedule_runs SET status='SUCCEEDED',artifact_uri=?,row_count=?,finished_at=now() WHERE id=? AND tenant_id=?",uri,rows.size(),run,s.tenant());
   advance(s,true,null);
  }catch(Exception ex){
   String message=ex.getMessage()==null?ex.getClass().getSimpleName():ex.getMessage();
   db.update("UPDATE sbase_bi_schedule_runs SET status='FAILED',error_message=?,finished_at=now() WHERE id=? AND tenant_id=?",message.substring(0,Math.min(4000,message.length())),run,s.tenant());
   advance(s,false,message);
  }
 }
 private void sendMail(String to,Schedule s,Path file,String uri,int count)throws Exception{
  JavaMailSender sender=mail.getIfAvailable();if(sender==null)throw new IllegalStateException("Email delivery requested but spring.mail.host is not configured");
  if(!to.matches("[A-Za-z0-9.!#$%&'*+/=?^_{|}~-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}"))throw new IllegalArgumentException("Invalid report recipient");
  var message=sender.createMimeMessage();MimeMessageHelper helper=new MimeMessageHelper(message,true,StandardCharsets.UTF_8.name());
  helper.setTo(to);helper.setSubject("SBase BI report: "+s.report());helper.setText("Scheduled BI report completed. Rows: "+count+"\\nArtifact: "+uri);
  helper.addAttachment(file.getFileName().toString(),file.toFile());sender.send(message);
 }
 private String csv(List<Map<String,Object>> rows){
  if(rows.isEmpty())return "";LinkedHashSet<String> columns=new LinkedHashSet<>();rows.forEach(r->columns.addAll(r.keySet()));
  StringBuilder out=new StringBuilder();out.append(String.join(",",columns.stream().map(this::cell).toList())).append("\r\n");
  for(Map<String,Object> row:rows)out.append(String.join(",",columns.stream().map(c->cell(Objects.toString(row.get(c),""))).toList())).append("\r\n");
  return out.toString();
 }
 private String cell(String value){return "\""+value.replace("\"","\"\"")+"\"";}
 private void advance(Schedule s,boolean success,String error){
  try{ZoneId zone=ZoneId.of(s.zone());ZonedDateTime next=CronExpression.parse(s.cron()).next(ZonedDateTime.now(zone));if(next==null)throw new IllegalStateException("No next schedule time");
   db.update("UPDATE sbase_bi_schedules SET next_run_at=?,last_run_at=now(),failure_count=CASE WHEN ? THEN 0 ELSE failure_count+1 END,enabled=CASE WHEN ? THEN enabled ELSE failure_count+1<5 END,lease_owner=NULL,lease_until=NULL WHERE id=? AND tenant_id=? AND lease_owner=?",next.toInstant(),success,success,s.id(),s.tenant(),node);
  }catch(Exception ex){db.update("UPDATE sbase_bi_schedules SET enabled=false,lease_owner=NULL,lease_until=NULL WHERE id=? AND tenant_id=? AND lease_owner=?",s.id(),s.tenant(),node);}
 }
 private record Schedule(UUID id,UUID tenant,UUID report,String owner,String cron,String zone,String format,String target){}
}
