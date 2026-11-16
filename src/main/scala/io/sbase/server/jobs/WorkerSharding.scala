package io.sbase.server.jobs
import org.apache.pekko.actor.typed.{ActorRef,ActorSystem,Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding,Entity,EntityTypeKey}
import io.sbase.server.engines.OperatorRegistry
import scala.concurrent.Future
import scala.util.{Failure,Success}

object WorkerSharding {
 sealed trait Command
 final case class Process(lease:JobLease) extends Command
 private final case class Finished(lease:JobLease,result:Either[Throwable,String]) extends Command
 val TypeKey:EntityTypeKey[Command]=EntityTypeKey[Command]("SBaseJobWorker")
 def start(system:ActorSystem[_],repo:JobRepository,operators:OperatorRegistry):ActorRef[Command] =
  ClusterSharding(system).init(Entity(TypeKey)(ctx=>worker(ctx.entityId,repo,operators,system.executionContext)))
 private def worker(id:String,repo:JobRepository,operators:OperatorRegistry,ec:scala.concurrent.ExecutionContext):Behavior[Command] =
  Behaviors.setup { context =>
   Behaviors.receiveMessage {
    case Process(lease) if lease.id.toString==id =>
     context.pipeToSelf(Future(operators.execute(lease.workflowDefinition).compactPrint)(ec)) {
      case Success(value)=>Finished(lease,Right(value))
      case Failure(error)=>Finished(lease,Left(error))
     }; Behaviors.same
    case Process(_)=>Behaviors.same
    case Finished(lease,Right(result))=>Future(repo.complete(lease,result))(ec);Behaviors.same
    case Finished(lease,Left(error))=>Future(repo.fail(lease,error))(ec);Behaviors.same
   }
  }
}
