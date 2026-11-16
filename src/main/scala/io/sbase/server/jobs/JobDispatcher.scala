package io.sbase.server.jobs
import org.apache.pekko.actor.typed.{ActorRef,ActorSystem}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import java.util.UUID

final class JobDispatcher(system:ActorSystem[_],repo:JobRepository,region:ActorRef[WorkerSharding.Command]) {
 private implicit val ec:ExecutionContext=system.executionContext
 private val owner=UUID.randomUUID()
 def start():Unit={
  system.scheduler.scheduleAtFixedRate(1.second,2.seconds)(()=>ec.execute(()=> {
   try repo.claim(owner,8).foreach(lease=>region ! WorkerSharding.Process(lease))
   catch {case error:Throwable=>system.log.error("Job dispatch cycle failed",error)}
  }))
 }
}
