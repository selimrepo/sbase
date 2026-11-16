package io.sbase.server.jobs

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.flywaydb.core.Flyway
import java.sql.Connection
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

final class JobRepository(dataSource: HikariDataSource, leaseSeconds: Int) {
 def migrate(): Unit = Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate()

 def claim(owner: UUID, limit: Int): Seq[JobLease] = {
  val c=dataSource.getConnection
  try {
   c.setAutoCommit(false)
   val sql="""WITH candidates AS (
    | SELECT id FROM job_execution
    | WHERE ((status IN ('QUEUED','RETRY_WAIT') AND next_run_at<=now())
    | OR (status='RUNNING' AND lease_expires_at<now()))
    | ORDER BY submitted_at FOR UPDATE SKIP LOCKED LIMIT ?
    |) UPDATE job_execution j SET status='RUNNING',lease_owner=?,
    | lease_expires_at=now()+(? * interval '1 second'),heartbeat_at=now(),
    | started_at=COALESCE(started_at,now()),attempt=j.attempt+1,updated_at=now()
    | FROM candidates x WHERE j.id=x.id
    | RETURNING j.id,j.attempt,j.max_attempts,
    | (SELECT v.definition::text FROM workflow_version v
    | WHERE v.workflow_id=j.workflow_id AND v.version=j.workflow_version) AS definition
    |""".stripMargin
   val p=c.prepareStatement(sql); p.setInt(1,math.max(1,limit)); p.setObject(2,owner); p.setInt(3,leaseSeconds)
   val rs=p.executeQuery(); val result=ArrayBuffer.empty[JobLease]
   while(rs.next()) {
    val id=rs.getObject("id",classOf[UUID]); val attempt=rs.getInt("attempt")
    result += JobLease(id,owner,attempt,rs.getInt("max_attempts"),rs.getString("definition"))
    event(c,id,"LEASE_ACQUIRED",attempt,"{}")
   }
   rs.close(); p.close(); c.commit(); result.toSeq
  } catch { case e:Throwable => c.rollback(); throw e } finally c.close()
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
