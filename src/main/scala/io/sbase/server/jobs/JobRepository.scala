package io.sbase.server.jobs

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.flywaydb.core.Flyway
import java.sql.Connection
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

final class JobRepository(dataSource: HikariDataSource, leaseSeconds: Int) {
 def migrate(): Unit = Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate()

 def claim(limit: Int): Seq[JobLease] = {
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val expire=c.createStatement()
   expire.executeUpdate("""WITH expired AS (
    | UPDATE job_execution SET status='FAILED',finished_at=now(),lease_owner=NULL,lease_expires_at=NULL,
    | error_code='LEASE_EXPIRED_MAX_ATTEMPTS',error_message='Worker lease expired after the final attempt',updated_at=now()
    | WHERE status='RUNNING' AND lease_expires_at<now() AND attempt>=max_attempts
    | RETURNING id,attempt
    |) INSERT INTO job_execution_event(job_id,event_type,attempt,details)
    | SELECT id,'FAILED_LEASE_EXPIRED',attempt,'{}'::jsonb FROM expired
    |""".stripMargin)
   expire.close()
   val sql="""WITH candidates AS (
    | SELECT id FROM job_execution
    | WHERE ((status IN ('QUEUED','RETRY_WAIT') AND next_run_at<=now())
    | OR (status='RUNNING' AND lease_expires_at<now()))
    | ORDER BY submitted_at LIMIT ? FOR UPDATE SKIP LOCKED
    |) UPDATE job_execution j SET status='RUNNING',lease_owner=gen_random_uuid(),
    | lease_expires_at=now()+(? * interval '1 second'),heartbeat_at=now(),
    | started_at=COALESCE(started_at,now()),attempt=j.attempt+1,error_code=NULL,error_message=NULL,updated_at=now()
    | FROM candidates x WHERE j.id=x.id
    | RETURNING j.id,j.lease_owner,j.attempt,j.max_attempts,
    | (SELECT v.definition::text FROM workflow_version v
    | WHERE v.workflow_id=j.workflow_id AND v.version=j.workflow_version) AS definition,
    | j.input_parameters::text AS input_parameters
    |""".stripMargin
   val p=c.prepareStatement(sql); p.setInt(1,math.max(1,limit)); p.setInt(2,leaseSeconds)
   val rs=p.executeQuery(); val result=ArrayBuffer.empty[JobLease]
   while(rs.next()) {
    val id=rs.getObject("id",classOf[UUID]); val attempt=rs.getInt("attempt")
    result += JobLease(id,rs.getObject("lease_owner",classOf[UUID]),attempt,rs.getInt("max_attempts"),rs.getString("definition"),rs.getString("input_parameters"))
    event(c,id,"LEASE_ACQUIRED",attempt,"{}")
   }
   rs.close(); p.close(); c.commit(); result.toSeq
  } catch { case e:Throwable => c.rollback(); throw e } finally c.close()
 }

 def heartbeat(lease:JobLease):Boolean = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("UPDATE job_execution SET heartbeat_at=now(),lease_expires_at=now()+(? * interval '1 second'),updated_at=now() WHERE id=? AND status='RUNNING' AND lease_owner=?")
   p.setInt(1,leaseSeconds);p.setObject(2,lease.id);p.setObject(3,lease.leaseOwner)
   val n=p.executeUpdate();p.close();n==1
  } finally c.close()
 }

 def createWorkflow(name:String,definition:String):(UUID,UUID) = {
  val c=dataSource.getConnection
  val workspaceId=UUID.randomUUID(); val workflowId=UUID.randomUUID()
  try {
   c.setAutoCommit(false)
   val w=c.prepareStatement("INSERT INTO workspace(id,name) VALUES (?,?)")
   w.setObject(1,workspaceId);w.setString(2,"Default workspace");w.executeUpdate();w.close()
   val wf=c.prepareStatement("INSERT INTO workflow(id,workspace_id,name) VALUES (?,?,?)")
   wf.setObject(1,workflowId);wf.setObject(2,workspaceId);wf.setString(3,name);wf.executeUpdate();wf.close()
   val version=c.prepareStatement("INSERT INTO workflow_version(workflow_id,version,definition,checksum) VALUES (?,1,?::jsonb,?)")
   version.setObject(1,workflowId);version.setString(2,definition)
   version.setString(3,java.security.MessageDigest.getInstance("SHA-256").digest(definition.getBytes("UTF-8")).map(b=>f"${b & 0xff}%02x").mkString)
   version.executeUpdate();version.close()
   val activate=c.prepareStatement("UPDATE workflow SET active_version=1 WHERE id=?")
   activate.setObject(1,workflowId);activate.executeUpdate();activate.close()
   c.commit();(workspaceId,workflowId)
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }


 def workflows(limit:Int):Seq[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("""SELECT w.id,w.workspace_id,w.name,w.description,w.active_version,w.created_at,w.updated_at,
    | (SELECT COUNT(*) FROM workflow_version v WHERE v.workflow_id=w.id) AS version_count,
    | (SELECT COUNT(*) FROM job_execution j WHERE j.workflow_id=w.id) AS run_count
    | FROM workflow w ORDER BY w.updated_at DESC LIMIT ?""".stripMargin)
   p.setInt(1,math.max(1,math.min(limit,500)))
   val rs=p.executeQuery();val out=ArrayBuffer.empty[Map[String,Object]]
   while(rs.next()) out += Map[String,Object](
    "id"->rs.getObject("id"),"workspaceId"->rs.getObject("workspace_id"),
    "name"->rs.getString("name"),"description"->Option(rs.getString("description")).orNull,
    "activeVersion"->(if(rs.getObject("active_version")==null)null else Int.box(rs.getInt("active_version"))),
    "versionCount"->Int.box(rs.getInt("version_count")),"runCount"->Int.box(rs.getInt("run_count")),
    "createdAt"->rs.getObject("created_at").toString,"updatedAt"->rs.getObject("updated_at").toString)
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 def workflow(id:UUID):Option[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("""SELECT w.id,w.workspace_id,w.name,w.description,w.active_version,w.created_at,w.updated_at,
    | v.checksum,v.definition::text AS definition,
    | (SELECT COUNT(*) FROM workflow_version x WHERE x.workflow_id=w.id) AS version_count,
    | (SELECT COUNT(*) FROM job_execution j WHERE j.workflow_id=w.id) AS run_count
    | FROM workflow w LEFT JOIN workflow_version v ON v.workflow_id=w.id AND v.version=w.active_version
    | WHERE w.id=?""".stripMargin)
   p.setObject(1,id);val rs=p.executeQuery()
   val result=if(rs.next())Some(Map[String,Object](
    "id"->rs.getObject("id"),"workspaceId"->rs.getObject("workspace_id"),
    "name"->rs.getString("name"),"description"->Option(rs.getString("description")).orNull,
    "activeVersion"->(if(rs.getObject("active_version")==null)null else Int.box(rs.getInt("active_version"))),
    "checksum"->Option(rs.getString("checksum")).orNull,"definition"->Option(rs.getString("definition")).orNull,
    "versionCount"->Int.box(rs.getInt("version_count")),"runCount"->Int.box(rs.getInt("run_count")),
    "createdAt"->rs.getObject("created_at").toString,"updatedAt"->rs.getObject("updated_at").toString)) else None
   rs.close();p.close();result
  } finally c.close()
 }

 def workflowVersion(id:UUID,version:Int):Option[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("SELECT version,checksum,definition::text,created_at FROM workflow_version WHERE workflow_id=? AND version=?")
   p.setObject(1,id);p.setInt(2,version);val rs=p.executeQuery()
   val result=if(rs.next())Some(Map[String,Object](
    "workflowId"->id.toString,"version"->Int.box(rs.getInt(1)),
    "checksum"->rs.getString(2),"definition"->rs.getString(3),
    "createdAt"->rs.getObject(4).toString))else None
   rs.close();p.close();result
  } finally c.close()
 }

 def jobs(limit:Int):Seq[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("""SELECT j.id,j.workflow_id,w.name AS workflow_name,j.workflow_version,j.status,
    | j.attempt,j.max_attempts,j.error_code,j.error_message,j.submitted_at,j.started_at,j.finished_at,j.result::text
    | FROM job_execution j JOIN workflow w ON w.id=j.workflow_id
    | ORDER BY j.submitted_at DESC LIMIT ?""".stripMargin)
   p.setInt(1,math.max(1,math.min(limit,500)));val rs=p.executeQuery();val out=ArrayBuffer.empty[Map[String,Object]]
   while(rs.next()) out += Map[String,Object](
    "id"->rs.getObject("id"),"workflowId"->rs.getObject("workflow_id"),
    "workflowName"->rs.getString("workflow_name"),"workflowVersion"->Int.box(rs.getInt("workflow_version")),
    "status"->rs.getString("status"),"attempt"->Int.box(rs.getInt("attempt")),
    "maxAttempts"->Int.box(rs.getInt("max_attempts")),"errorCode"->Option(rs.getString("error_code")).orNull,
    "errorMessage"->Option(rs.getString("error_message")).orNull,
    "submittedAt"->rs.getObject("submitted_at").toString,
    "startedAt"->Option(rs.getString("started_at")).orNull,
    "finishedAt"->Option(rs.getString("finished_at")).orNull,
    "result"->Option(rs.getString("result")).orNull)
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 def submit(workflowId:UUID,idempotencyKey:String,maxAttempts:Int):UUID = {
  require(idempotencyKey!=null && idempotencyKey.nonEmpty && idempotencyKey.length<=200,"Invalid idempotency key")
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val id=UUID.randomUUID()
   val p=c.prepareStatement("""INSERT INTO job_execution(id,workflow_id,workflow_version,status,idempotency_key,correlation_id,max_attempts)
    |SELECT ?,id,active_version,'QUEUED',?,?,? FROM workflow WHERE id=? AND active_version IS NOT NULL
    |ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id""".stripMargin)
   p.setObject(1,id);p.setString(2,idempotencyKey);p.setObject(3,UUID.randomUUID())
   p.setInt(4,math.max(1,math.min(maxAttempts,20)));p.setObject(5,workflowId)
   val rs=p.executeQuery()
   val created=rs.next()
   val jobId=if(created)rs.getObject(1,classOf[UUID]) else {
    val find=c.prepareStatement("SELECT id FROM job_execution WHERE idempotency_key=?")
    find.setString(1,idempotencyKey);val existing=find.executeQuery()
    if(!existing.next()) throw new IllegalStateException("Unable to resolve idempotent job submission")
    val existingId=existing.getObject(1,classOf[UUID]);existing.close();find.close();existingId
   }
   rs.close();p.close()
   if(created) event(c,jobId,"QUEUED",0,"{}")
   c.commit();jobId
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }


 def workflowRuns(workflowId:UUID,limit:Int):Seq[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("SELECT id,workflow_version,status,attempt,max_attempts,error_code,error_message,submitted_at,started_at,finished_at,result::text FROM job_execution WHERE workflow_id=? ORDER BY submitted_at DESC LIMIT ?")
   p.setObject(1,workflowId);p.setInt(2,math.max(1,math.min(limit,500)));val rs=p.executeQuery();val out=ArrayBuffer.empty[Map[String,Object]]
   while(rs.next()) out += Map[String,Object]("jobId"->rs.getObject(1),"workflowVersion"->Int.box(rs.getInt(2)),"status"->rs.getString(3),"attempt"->Int.box(rs.getInt(4)),"maxAttempts"->Int.box(rs.getInt(5)),"errorCode"->Option(rs.getString(6)).orNull,"errorMessage"->Option(rs.getString(7)).orNull,"submittedAt"->rs.getString(8),"startedAt"->Option(rs.getString(9)).orNull,"finishedAt"->Option(rs.getString(10)).orNull,"result"->Option(rs.getString(11)).orNull)
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 def jobEvents(jobId:UUID):Seq[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("SELECT event_type,attempt,details::text,created_at FROM job_execution_event WHERE job_id=? ORDER BY created_at,id")
   p.setObject(1,jobId);val rs=p.executeQuery();val out=ArrayBuffer.empty[Map[String,Object]]
   while(rs.next()) out += Map[String,Object]("eventType"->rs.getString(1),"attempt"->Int.box(rs.getInt(2)),"details"->rs.getString(3),"createdAt"->rs.getString(4))
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 def status(id:UUID):Option[(String,Int,Option[String],Option[String])] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("SELECT status,attempt,error_message,result::text FROM job_execution WHERE id=?")
   p.setObject(1,id);val rs=p.executeQuery()
   val result=if(rs.next())Some((rs.getString(1),rs.getInt(2),Option(rs.getString(3)),Option(rs.getString(4))))else None
   rs.close();p.close();result
  } finally c.close()
 }

 def complete(lease:JobLease,result:String):Boolean = {
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val p=c.prepareStatement("""UPDATE job_execution SET status='SUCCEEDED',result=?::jsonb,
    |finished_at=now(),lease_owner=NULL,lease_expires_at=NULL,error_code=NULL,error_message=NULL,updated_at=now()
    |WHERE id=? AND status='RUNNING' AND lease_owner=?""".stripMargin)
   p.setString(1,result); p.setObject(2,lease.id); p.setObject(3,lease.leaseOwner)
   val n=p.executeUpdate(); if(n==1) event(c,lease.id,"SUCCEEDED",lease.attempt,"{}")
   c.commit(); n==1
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }

 def fail(lease:JobLease,error:Throwable):Boolean = {
  val retry=lease.attempt<lease.maxAttempts
  val delay=math.min(300L,math.pow(2.0,math.min(lease.attempt,8)).toLong)
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val p=c.prepareStatement("""UPDATE job_execution SET status=?,next_run_at=now()+(? * interval '1 second'),
    |finished_at=CASE WHEN ? THEN NULL ELSE now() END,lease_owner=NULL,lease_expires_at=NULL,
    |error_code='OPERATOR_FAILED',error_message=?,updated_at=now()
    |WHERE id=? AND status='RUNNING' AND lease_owner=?""".stripMargin)
   p.setString(1,if(retry)"RETRY_WAIT" else "FAILED"); p.setLong(2,if(retry)delay else 0L)
   p.setBoolean(3,retry); p.setString(4,Option(error.getMessage).getOrElse(error.getClass.getSimpleName).take(4000))
   p.setObject(5,lease.id); p.setObject(6,lease.leaseOwner)
   val n=p.executeUpdate(); if(n==1) event(c,lease.id,if(retry)"RETRY_SCHEDULED" else "FAILED",lease.attempt,"{}")
   c.commit(); n==1
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }

 private def event(c:Connection,id:UUID,name:String,attempt:Int,details:String):Unit = {
  val p=c.prepareStatement("INSERT INTO job_execution_event(job_id,event_type,attempt,details) VALUES (?,?,?,?::jsonb)")
  p.setObject(1,id);p.setString(2,name);p.setInt(3,attempt);p.setString(4,details);p.executeUpdate();p.close()
 }

 def createWorkflowVersion(workflowId:UUID,definition:String,activate:Boolean):Int = {
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val lock=c.prepareStatement("SELECT id FROM workflow WHERE id=? FOR UPDATE")
   lock.setObject(1,workflowId);val exists=lock.executeQuery()
   if(!exists.next()) throw new NoSuchElementException("Workflow not found")
   exists.close();lock.close()
   val q=c.prepareStatement("SELECT COALESCE(MAX(version),0)+1 FROM workflow_version WHERE workflow_id=?")
   q.setObject(1,workflowId);val rs=q.executeQuery();rs.next();val version=rs.getInt(1);rs.close();q.close()
   val p=c.prepareStatement("INSERT INTO workflow_version(workflow_id,version,definition,checksum) VALUES (?, ?, ?::jsonb, ?)")
   p.setObject(1,workflowId);p.setInt(2,version);p.setString(3,definition)
   p.setString(4,java.security.MessageDigest.getInstance("SHA-256").digest(definition.getBytes("UTF-8")).map(b=>f"${b & 0xff}%02x").mkString)
   p.executeUpdate();p.close()
   if(activate) { val a=c.prepareStatement("UPDATE workflow SET active_version=?,updated_at=now() WHERE id=?");a.setInt(1,version);a.setObject(2,workflowId);a.executeUpdate();a.close() }
   c.commit();version
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }

 def activateWorkflowVersion(workflowId:UUID,version:Int):Boolean = {
  val c=dataSource.getConnection
  try { val p=c.prepareStatement("UPDATE workflow SET active_version=?,updated_at=now() WHERE id=? AND EXISTS (SELECT 1 FROM workflow_version v WHERE v.workflow_id=? AND v.version=?)")
   p.setInt(1,version);p.setObject(2,workflowId);p.setObject(3,workflowId);p.setInt(4,version)
   val changed=p.executeUpdate();p.close();changed==1
  } finally c.close()
 }

 def workflowVersions(workflowId:UUID):Seq[(Int,String,String,java.time.OffsetDateTime)] = {
  val c=dataSource.getConnection
  try { val p=c.prepareStatement("SELECT v.version,v.checksum,v.definition::text,v.created_at FROM workflow_version v WHERE v.workflow_id=? ORDER BY v.version DESC")
   p.setObject(1,workflowId);val rs=p.executeQuery();val out=ArrayBuffer.empty[(Int,String,String,java.time.OffsetDateTime)]
   while(rs.next()) out += ((rs.getInt(1),rs.getString(2),rs.getString(3),rs.getObject(4,classOf[java.time.OffsetDateTime])))
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 def createSchedule(workflowId:UUID,intervalSeconds:Int,maxAttempts:Int):UUID = {
  require(intervalSeconds>=10 && intervalSeconds<=31536000,"Schedule interval must be between 10 seconds and 365 days")
  val c=dataSource.getConnection
  try { val id=UUID.randomUUID()
   val p=c.prepareStatement("INSERT INTO workflow_schedule(id,workflow_id,workflow_version,interval_seconds,max_attempts,next_run_at) SELECT ?,id,active_version,?,?,now() FROM workflow WHERE id=? AND active_version IS NOT NULL")
   p.setObject(1,id);p.setInt(2,intervalSeconds);p.setInt(3,math.max(1,math.min(maxAttempts,20)));p.setObject(4,workflowId)
   val n=p.executeUpdate();p.close();if(n==0)throw new NoSuchElementException("Workflow not found or has no active version");id
  } finally c.close()
 }

 def schedules(workflowId:Option[UUID]):Seq[Map[String,Object]] = {
  val c=dataSource.getConnection
  try { val sql="SELECT id,workflow_id,workflow_version,interval_seconds,max_attempts,enabled,next_run_at,last_triggered_at FROM workflow_schedule "+(if(workflowId.isDefined)"WHERE workflow_id=? " else "")+"ORDER BY created_at DESC"
   val p=c.prepareStatement(sql);workflowId.foreach(p.setObject(1,_));val rs=p.executeQuery();val out=ArrayBuffer.empty[Map[String,Object]]
   while(rs.next()) out += Map[String,Object]("id"->rs.getObject(1),"workflowId"->rs.getObject(2),"workflowVersion"->Int.box(rs.getInt(3)),"intervalSeconds"->Int.box(rs.getInt(4)),"maxAttempts"->Int.box(rs.getInt(5)),"enabled"->Boolean.box(rs.getBoolean(6)),"nextRunAt"->Option(rs.getString(7)).orNull,"lastTriggeredAt"->Option(rs.getString(8)).orNull)
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 def setScheduleEnabled(id:UUID,enabled:Boolean):Boolean = {
  val c=dataSource.getConnection
  try { val p=c.prepareStatement("UPDATE workflow_schedule SET enabled=?,next_run_at=CASE WHEN ? THEN now() ELSE next_run_at END,updated_at=now() WHERE id=?")
   p.setBoolean(1,enabled);p.setBoolean(2,enabled);p.setObject(3,id);val n=p.executeUpdate();p.close();n==1
  } finally c.close()
 }

 def dispatchDueSchedules(limit:Int):Int = {
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val q=c.prepareStatement("SELECT id,workflow_id,workflow_version,interval_seconds,max_attempts,next_run_at FROM workflow_schedule WHERE enabled=TRUE AND next_run_at<=now() ORDER BY next_run_at LIMIT ? FOR UPDATE SKIP LOCKED")
   q.setInt(1,math.max(1,limit));val rs=q.executeQuery()
   val due=ArrayBuffer.empty[(UUID,UUID,Int,Int,Int,java.time.OffsetDateTime)]
   while(rs.next()) due += ((rs.getObject(1,classOf[UUID]),rs.getObject(2,classOf[UUID]),rs.getInt(3),rs.getInt(4),rs.getInt(5),rs.getObject(6,classOf[java.time.OffsetDateTime])))
   rs.close();q.close()
   due.foreach { case (id,wf,version,interval,attempts,next)=>
    val job=UUID.randomUUID()
    val p=c.prepareStatement("INSERT INTO job_execution(id,workflow_id,workflow_version,status,idempotency_key,correlation_id,max_attempts) VALUES (?, ?, ?, 'QUEUED', ?, ?, ?) ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING")
    p.setObject(1,job);p.setObject(2,wf);p.setInt(3,version);p.setString(4,"schedule:"+id.toString+":"+next.toInstant.toEpochMilli);p.setObject(5,UUID.randomUUID());p.setInt(6,attempts)
    val created=p.executeUpdate()==1;p.close()
    if(created) event(c,job,"QUEUED_BY_SCHEDULE",0,"{}")
    val u=c.prepareStatement("UPDATE workflow_schedule SET next_run_at=now()+(? * interval '1 second'),last_triggered_at=now(),updated_at=now() WHERE id=?")
    u.setInt(1,interval);u.setObject(2,id);u.executeUpdate();u.close()
   }
   c.commit();due.size
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }


 /** Publish an immutable workflow version as a stable REST-callable service. */
 def publishDeployment(workflowId:UUID,slug:String,requestedVersion:Int,maxConcurrentRuns:Int,maxAttempts:Int):UUID = {
  require(slug!=null && slug.matches("[a-z0-9][a-z0-9-]{1,118}[a-z0-9]"),"Deployment slug must be 3..120 lowercase letters, digits or hyphens")
  require(requestedVersion>=0,"version must be zero (active) or a positive immutable version")
  require(maxConcurrentRuns>=1 && maxConcurrentRuns<=100,"maxConcurrentRuns must be between 1 and 100")
  require(maxAttempts>=1 && maxAttempts<=20,"maxAttempts must be between 1 and 20")
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val wf=c.prepareStatement("SELECT workspace_id,active_version FROM workflow WHERE id=? FOR UPDATE")
   wf.setObject(1,workflowId);val wr=wf.executeQuery()
   if(!wr.next()) throw new NoSuchElementException("Workflow not found")
   val workspaceId=wr.getObject(1,classOf[UUID])
   val active=wr.getInt(2);val activeWasNull=wr.wasNull()
   wr.close();wf.close()
   val version=if(requestedVersion<=0) { if(activeWasNull) throw new IllegalStateException("Workflow has no active version"); active } else requestedVersion
   val exists=c.prepareStatement("SELECT 1 FROM workflow_version WHERE workflow_id=? AND version=?")
   exists.setObject(1,workflowId);exists.setInt(2,version);val er=exists.executeQuery()
   val versionExists=er.next();er.close();exists.close()
   if(!versionExists) throw new NoSuchElementException("Workflow version not found")
   val p=c.prepareStatement("""INSERT INTO workflow_deployment(id,workflow_id,workflow_version,slug,max_concurrent_runs,max_attempts)
    | VALUES (?,?,?,?,?,?)
    | ON CONFLICT (slug) DO UPDATE SET workflow_version=EXCLUDED.workflow_version,
    | max_concurrent_runs=EXCLUDED.max_concurrent_runs,max_attempts=EXCLUDED.max_attempts,
    | enabled=TRUE,updated_at=now()
    | WHERE workflow_deployment.workflow_id=EXCLUDED.workflow_id
    | RETURNING id""".stripMargin)
   p.setObject(1,UUID.randomUUID());p.setObject(2,workflowId);p.setInt(3,version);p.setString(4,slug)
   p.setInt(5,maxConcurrentRuns);p.setInt(6,maxAttempts)
   val rs=p.executeQuery()
   if(!rs.next()) throw new IllegalArgumentException("Deployment slug is already assigned to another workflow")
   val id=rs.getObject(1,classOf[UUID]);rs.close();p.close()
   audit(c,workspaceId,workflowId,"DEPLOYMENT_PUBLISHED",
     "{\"deploymentId\":\""+id+"\",\"slug\":\""+slug+"\",\"version\":"+version+"}")
   c.commit();id
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }

 def deployments(workflowId:Option[UUID],limit:Int):Seq[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val sql="""SELECT d.id,d.workflow_id,w.name AS workflow_name,d.workflow_version,d.slug,d.enabled,
    | d.max_concurrent_runs,d.max_attempts,d.invocation_count,d.last_invoked_at,d.created_at,d.updated_at,
    | (SELECT COUNT(*) FROM job_execution j WHERE j.deployment_id=d.id
    |  AND j.status IN ('QUEUED','RUNNING','RETRY_WAIT')) AS active_runs
    | FROM workflow_deployment d JOIN workflow w ON w.id=d.workflow_id """+
    (if(workflowId.isDefined)"WHERE d.workflow_id=? " else "")+"ORDER BY d.updated_at DESC LIMIT ?"
   val p=c.prepareStatement(sql);var i=1;workflowId.foreach{id=>p.setObject(i,id);i+=1};p.setInt(i,math.max(1,math.min(limit,500)))
   val rs=p.executeQuery();val out=ArrayBuffer.empty[Map[String,Object]]
   while(rs.next()) out+=Map[String,Object](
    "id"->rs.getObject("id"),"workflowId"->rs.getObject("workflow_id"),"workflowName"->rs.getString("workflow_name"),
    "workflowVersion"->Int.box(rs.getInt("workflow_version")),"slug"->rs.getString("slug"),
    "enabled"->Boolean.box(rs.getBoolean("enabled")),"maxConcurrentRuns"->Int.box(rs.getInt("max_concurrent_runs")),
    "maxAttempts"->Int.box(rs.getInt("max_attempts")),"invocationCount"->Long.box(rs.getLong("invocation_count")),
    "activeRuns"->Int.box(rs.getInt("active_runs")),"lastInvokedAt"->Option(rs.getString("last_invoked_at")).orNull,
    "createdAt"->rs.getString("created_at"),"updatedAt"->rs.getString("updated_at"))
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 def deployment(slug:String):Option[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("""SELECT d.id,d.workflow_id,w.name AS workflow_name,d.workflow_version,d.slug,d.enabled,
    | d.max_concurrent_runs,d.max_attempts,d.invocation_count,d.last_invoked_at,d.created_at,d.updated_at,
    | (SELECT COUNT(*) FROM job_execution j WHERE j.deployment_id=d.id
    |  AND j.status IN ('QUEUED','RUNNING','RETRY_WAIT')) AS active_runs
    | FROM workflow_deployment d JOIN workflow w ON w.id=d.workflow_id WHERE d.slug=?""".stripMargin)
   p.setString(1,slug);val rs=p.executeQuery()
   val result=if(rs.next())Some(Map[String,Object](
    "id"->rs.getObject("id"),"workflowId"->rs.getObject("workflow_id"),"workflowName"->rs.getString("workflow_name"),
    "workflowVersion"->Int.box(rs.getInt("workflow_version")),"slug"->rs.getString("slug"),
    "enabled"->Boolean.box(rs.getBoolean("enabled")),"maxConcurrentRuns"->Int.box(rs.getInt("max_concurrent_runs")),
    "maxAttempts"->Int.box(rs.getInt("max_attempts")),"invocationCount"->Long.box(rs.getLong("invocation_count")),
    "activeRuns"->Int.box(rs.getInt("active_runs")),"lastInvokedAt"->Option(rs.getString("last_invoked_at")).orNull,
    "createdAt"->rs.getString("created_at"),"updatedAt"->rs.getString("updated_at")))else None
   rs.close();p.close();result
  } finally c.close()
 }

 def setDeploymentEnabled(slug:String,enabled:Boolean):Boolean = {
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val p=c.prepareStatement("""UPDATE workflow_deployment d SET enabled=?,updated_at=now()
    | FROM workflow w WHERE d.workflow_id=w.id AND d.slug=?
    | RETURNING d.id,d.workflow_id,w.workspace_id""".stripMargin)
   p.setBoolean(1,enabled);p.setString(2,slug);val rs=p.executeQuery()
   if(!rs.next()){rs.close();p.close();c.rollback();return false}
   val deploymentId=rs.getObject(1,classOf[UUID]);val workflowId=rs.getObject(2,classOf[UUID]);val workspaceId=rs.getObject(3,classOf[UUID])
   rs.close();p.close()
   audit(c,workspaceId,workflowId,if(enabled)"DEPLOYMENT_RESUMED" else "DEPLOYMENT_PAUSED",
    "{\"deploymentId\":\""+deploymentId+"\",\"slug\":\""+slug+"\"}")
   c.commit();true
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }

 /** Atomic per-deployment concurrency admission, pinned-version queueing and idempotency. */
 def invokeDeployment(slug:String,idempotencyKey:String,requestedAttempts:Int,parametersJson:String="{}"):(UUID,Int) = {
  require(idempotencyKey!=null && idempotencyKey.nonEmpty && idempotencyKey.length<=150,"idempotencyKey must be 1..150 characters")
  require(slug!=null && slug.matches("[a-z0-9][a-z0-9-]{1,118}[a-z0-9]"),"Invalid deployment slug")
  val scopedKey="deployment:"+slug+":"+idempotencyKey
  require(scopedKey.length<=200,"Combined deployment slug and idempotency key must fit the 200-character durable key limit")
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val p=c.prepareStatement("""SELECT d.id,d.workflow_id,d.workflow_version,d.max_concurrent_runs,d.max_attempts,
    | d.enabled,w.workspace_id FROM workflow_deployment d JOIN workflow w ON w.id=d.workflow_id
    | WHERE d.slug=? FOR UPDATE OF d""".stripMargin)
   p.setString(1,slug);val rs=p.executeQuery()
   if(!rs.next()) throw new NoSuchElementException("Deployment not found")
   val deploymentId=rs.getObject(1,classOf[UUID]);val workflowId=rs.getObject(2,classOf[UUID])
   val version=rs.getInt(3);val maxConcurrent=rs.getInt(4);val configuredAttempts=rs.getInt(5)
   val enabled=rs.getBoolean(6);val workspaceId=rs.getObject(7,classOf[UUID])
   rs.close();p.close()
   if(!enabled) throw new IllegalStateException("Deployment is paused")
   val existing=c.prepareStatement("SELECT id,workflow_version,(input_parameters=?::jsonb) AS same_payload FROM job_execution WHERE idempotency_key=?")
   existing.setString(1,parametersJson);existing.setString(2,scopedKey);val er=existing.executeQuery()
   if(er.next()) {
    val samePayload=er.getBoolean(3)
    val result=(er.getObject(1,classOf[UUID]),er.getInt(2));er.close();existing.close()
    if(!samePayload) throw new IllegalArgumentException("Idempotency key was already used with different parameters")
    c.commit();return result
   }
   er.close();existing.close()
   val count=c.prepareStatement("SELECT COUNT(*) FROM job_execution WHERE deployment_id=? AND status IN ('QUEUED','RUNNING','RETRY_WAIT')")
   count.setObject(1,deploymentId);val cr=count.executeQuery();cr.next();val active=cr.getInt(1);cr.close();count.close()
   if(active>=maxConcurrent) throw new IllegalStateException("Deployment concurrency limit reached")
   val jobId=UUID.randomUUID()
   val insert=c.prepareStatement("""INSERT INTO job_execution(id,workflow_id,workflow_version,status,idempotency_key,correlation_id,max_attempts,deployment_id,input_parameters)
    | VALUES (?, ?, ?, 'QUEUED', ?, ?, ?, ?, ?::jsonb)""".stripMargin)
   insert.setObject(1,jobId);insert.setObject(2,workflowId);insert.setInt(3,version);insert.setString(4,scopedKey)
   insert.setObject(5,UUID.randomUUID());insert.setInt(6,math.max(1,math.min(requestedAttempts,configuredAttempts)));insert.setObject(7,deploymentId);insert.setString(8,parametersJson)
   insert.executeUpdate();insert.close()
   event(c,jobId,"QUEUED_BY_DEPLOYMENT",0,"{\"deploymentId\":\""+deploymentId+"\",\"slug\":\""+slug+"\"}")
   val update=c.prepareStatement("UPDATE workflow_deployment SET invocation_count=invocation_count+1,last_invoked_at=now(),updated_at=now() WHERE id=?")
   update.setObject(1,deploymentId);update.executeUpdate();update.close()
   audit(c,workspaceId,workflowId,"DEPLOYMENT_INVOKED",
    "{\"deploymentId\":\""+deploymentId+"\",\"slug\":\""+slug+"\",\"jobId\":\""+jobId+"\",\"version\":"+version+"}")
   c.commit();(jobId,version)
  } catch {case e:Throwable=>c.rollback();throw e} finally c.close()
 }


 def deploymentRuns(slug:String,limit:Int):Seq[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("""SELECT j.id,j.workflow_version,j.status,j.attempt,j.max_attempts,
    | j.error_code,j.error_message,j.submitted_at,j.started_at,j.finished_at,j.result::text
    | FROM job_execution j JOIN workflow_deployment d ON d.id=j.deployment_id
    | WHERE d.slug=? ORDER BY j.submitted_at DESC LIMIT ?""".stripMargin)
   p.setString(1,slug);p.setInt(2,math.max(1,math.min(limit,500)))
   val rs=p.executeQuery();val out=ArrayBuffer.empty[Map[String,Object]]
   while(rs.next())out+=Map[String,Object](
    "jobId"->rs.getObject(1),"workflowVersion"->Int.box(rs.getInt(2)),"status"->rs.getString(3),
    "attempt"->Int.box(rs.getInt(4)),"maxAttempts"->Int.box(rs.getInt(5)),
    "errorCode"->Option(rs.getString(6)).orNull,"errorMessage"->Option(rs.getString(7)).orNull,
    "submittedAt"->rs.getString(8),"startedAt"->Option(rs.getString(9)).orNull,
    "finishedAt"->Option(rs.getString(10)).orNull,"result"->Option(rs.getString(11)).orNull)
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 def workflowAudit(workflowId:UUID,limit:Int):Seq[Map[String,Object]] = {
  val c=dataSource.getConnection
  try {
   val p=c.prepareStatement("SELECT event_type,actor_id,details::text,created_at FROM workflow_audit_event WHERE workflow_id=? ORDER BY created_at DESC,id DESC LIMIT ?")
   p.setObject(1,workflowId);p.setInt(2,math.max(1,math.min(limit,500)))
   val rs=p.executeQuery();val out=ArrayBuffer.empty[Map[String,Object]]
   while(rs.next())out+=Map[String,Object]("eventType"->rs.getString(1),"actorId"->rs.getString(2),
    "details"->rs.getString(3),"createdAt"->rs.getString(4))
   rs.close();p.close();out.toSeq
  } finally c.close()
 }

 private def audit(c:Connection,workspaceId:UUID,workflowId:UUID,eventType:String,details:String):Unit = {
  val p=c.prepareStatement("INSERT INTO workflow_audit_event(workspace_id,workflow_id,event_type,actor_id,details) VALUES (?,?,?,'system',?::jsonb)")
  p.setObject(1,workspaceId);p.setObject(2,workflowId);p.setString(3,eventType);p.setString(4,details);p.executeUpdate();p.close()
 }

 def isHealthy:Boolean = {
  val c=dataSource.getConnection
  try { val s=c.createStatement(); val rs=s.executeQuery("SELECT 1"); val ok=rs.next();rs.close();s.close();ok } finally c.close()
 }
 def close():Unit=dataSource.close()
}
object JobRepository {
 def fromEnvironment():JobRepository = {
  val c=new HikariConfig()
  c.setJdbcUrl(sys.env.getOrElse("SBASE_POSTGRES_URL","jdbc:postgresql://localhost:5432/sbase"))
  c.setUsername(sys.env.getOrElse("SBASE_POSTGRES_USER","sbase"))
  c.setPassword(sys.env.getOrElse("SBASE_POSTGRES_PASSWORD","change-me"))
  c.setMaximumPoolSize(sys.env.get("SBASE_DB_POOL_SIZE").flatMap(_.toIntOption).getOrElse(16))
  c.setMinimumIdle(2);c.setPoolName("sbase-control-plane")
  new JobRepository(new HikariDataSource(c),sys.env.get("SBASE_JOB_LEASE_SECONDS").flatMap(_.toIntOption).getOrElse(60))
 }
}
