package io.sbase.server.jobs
import org.apache.pekko.actor.typed.{ActorRef,ActorSystem}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final class JobDispatcher(system:ActorSystem[_],repo:JobRepository,region:ActorRef[ShardingEnvelope[WorkerSharding.Command]]) {
 private implicit val ec:ExecutionContext=system.executionContext
 def start():Unit={
  system.scheduler.scheduleAtFixedRate(1.second,2.seconds)(()=>ec.execute(()=> {
   try repo.claim(8).foreach(lease=>region ! ShardingEnvelope(lease.id.toString,WorkerSharding.Process(lease)))
   catch {case error:Throwable=>system.log.error("Job dispatch cycle failed",error)}
  }))
 }
}
