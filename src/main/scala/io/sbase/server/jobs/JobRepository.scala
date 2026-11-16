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
    | started_at=COALESCE(started_at,now()),attempt=j.attempt+1,updated_at=now()
    | FROM candidates x WHERE j.id=x.id
    | RETURNING j.id,j.lease_owner,j.attempt,j.max_attempts,
    | (SELECT v.definition::text FROM workflow_version v
    | WHERE v.workflow_id=j.workflow_id AND v.version=j.workflow_version) AS definition
    |""".stripMargin
   val p=c.prepareStatement(sql); p.setInt(1,math.max(1,limit)); p.setInt(2,leaseSeconds)
   val rs=p.executeQuery(); val result=ArrayBuffer.empty[JobLease]
   while(rs.next()) {
    val id=rs.getObject("id",classOf[UUID]); val attempt=rs.getInt("attempt")
    result += JobLease(id,rs.getObject("lease_owner",classOf[UUID]),attempt,rs.getInt("max_attempts"),rs.getString("definition"))
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

 def status(id:UUID):Option[(String,Int,Option[String])] = {
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
    |finished_at=now(),lease_owner=NULL,lease_expires_at=NULL,updated_at=now()
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
