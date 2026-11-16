package io.sbase.server.jobs

import org.apache.pekko.actor.typed.{ActorRef,ActorSystem,Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.Cancellable
import org.apache.pekko.serialization.jackson.JacksonJsonSerializable
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding,Entity,EntityTypeKey}
import io.sbase.server.engines.OperatorRegistry
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure,Success}

object WorkerSharding {
 sealed trait Command extends JacksonJsonSerializable
 final case class Process(lease:JobLease) extends Command
 private final case class Finished(lease:JobLease,result:Either[String,String]) extends Command
 val TypeKey:EntityTypeKey[Command]=EntityTypeKey[Command]("SBaseJobWorker")

 def start(system:ActorSystem[_],repo:JobRepository,operators:OperatorRegistry):ActorRef[Command] =
  ClusterSharding(system).init(Entity(TypeKey)(ctx=>worker(ctx.entityId,repo,operators,system.executionContext)))

 private def worker(id:String,repo:JobRepository,operators:OperatorRegistry,ec:scala.concurrent.ExecutionContext):Behavior[Command] =
  Behaviors.setup { context =>
   var heartbeat:Cancellable=null
   Behaviors.receiveMessage {
    case Process(lease) if lease.id.toString==id =>
     if(heartbeat!=null) heartbeat.cancel()
     heartbeat=context.system.scheduler.scheduleAtFixedRate(15.seconds,15.seconds)(() => Future(repo.heartbeat(lease))(ec))(ec)
     context.pipeToSelf(Future(operators.execute(lease.workflowDefinition).compactPrint)(ec)) {
      case Success(value)=>Finished(lease,Right(value))
      case Failure(error)=>Finished(lease,Left(Option(error.getMessage).getOrElse(error.getClass.getName)))
     }
     Behaviors.same
    case Process(_)=>Behaviors.same
    case Finished(lease,Right(result))=>
     if(heartbeat!=null) heartbeat.cancel()
     Future(repo.complete(lease,result))(ec)
     Behaviors.same
    case Finished(lease,Left(error))=>
     if(heartbeat!=null) heartbeat.cancel()
     Future(repo.fail(lease,new RuntimeException(error)))(ec)
     Behaviors.same
   }
  }
}
