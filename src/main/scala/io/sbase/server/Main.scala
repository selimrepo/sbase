package io.sbase.server

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import spray.json.DefaultJsonProtocol._
import io.sbase.server.jobs.{JobDispatcher,JobRepository,WorkerSharding}
import io.sbase.server.engines.OperatorRegistry
import scala.concurrent.{ExecutionContext,Future}
import scala.util.{Failure,Success}
import java.util.UUID

object Main {
 final case class ApiInfo(name:String,version:String,status:String)
 final case class Health(status:String,service:String)
 final case class WorkflowCreate(name:String,definition:JsValue)
 final case class WorkflowCreated(workspaceId:String,workflowId:String)
 final case class JobSubmit(idempotencyKey:String,maxAttempts:Int=5)
 final case class JobAccepted(jobId:String)
 final case class JobStatus(status:String,attempt:Int,error:Option[String])

 def ApiInfoName():String="SBase Data Science Server"

 def main(args:Array[String]):Unit={
  implicit val system:ActorSystem[Nothing]=ActorSystem(Behaviors.empty,"sbase")
  implicit val ec:ExecutionContext=system.executionContext
  implicit val apiInfoFormat=jsonFormat3(ApiInfo.apply)
  implicit val healthFormat=jsonFormat2(Health.apply)
  implicit val workflowCreateFormat=jsonFormat2(WorkflowCreate.apply)
  implicit val workflowCreatedFormat=jsonFormat2(WorkflowCreated.apply)
  implicit val jobSubmitFormat=jsonFormat2(JobSubmit.apply)
  implicit val jobAcceptedFormat=jsonFormat1(JobAccepted.apply)
  implicit val jobStatusFormat=jsonFormat3(JobStatus.apply)

  val repository=JobRepository.fromEnvironment()
  repository.migrate()
  val operators=new OperatorRegistry()
  val workerRegion=WorkerSharding.start(system,repository,operators)
  new JobDispatcher(system,repository,workerRegion).start()

  val host=sys.env.getOrElse("SBASE_HTTP_HOST","0.0.0.0")
  val port=sys.env.get("SBASE_HTTP_PORT").flatMap(_.toIntOption).getOrElse(8080)

  val route=
   path("health"){get{complete(Health("UP","sbase-server"))}} ~
   pathPrefix("api"/"v1"){
    pathEndOrSingleSlash{get{complete(ApiInfo(ApiInfoName(),"0.2.0-SNAPSHOT","WORKFLOW_ENGINE"))}} ~
    path("ready"){get{complete(Health("UP","postgres-and-pekko-sharding"))}} ~
    path("workflows"){
     post{
      entity(as[WorkflowCreate]){request=>
       val name=request.name.trim
       if(name.isEmpty) complete(StatusCodes.BadRequest)
       else {
        val definition=request.definition.compactPrint
        operators.validate(definition)
        val future=Future(scala.concurrent.blocking(repository.createWorkflow(name,definition)))
        onComplete(future){
         case Success((workspaceId,workflowId))=>complete(StatusCodes.Created->WorkflowCreated(workspaceId.toString,workflowId.toString))
         case Failure(error)=>system.log.error("Workflow creation failed",error);complete(StatusCodes.InternalServerError)
        }
       }
      }
     }
    } ~
    path("workflows"/JavaUUID/"runs"){workflowId=>
     post{
      entity(as[JobSubmit]){request=>
       val key=Option(request.idempotencyKey).map(_.trim).filter(_.nonEmpty).getOrElse(UUID.randomUUID().toString)
       onComplete(Future(scala.concurrent.blocking(repository.submit(workflowId,key,request.maxAttempts)))){
        case Success(jobId)=>complete(StatusCodes.Accepted->JobAccepted(jobId.toString))
        case Failure(error)=>system.log.error("Job submission failed",error);complete(StatusCodes.BadRequest)
       }
      }
     }
    } ~
    path("jobs"/JavaUUID){jobId=>
     get{
      onComplete(Future(scala.concurrent.blocking(repository.status(jobId)))){
       case Success(Some((status,attempt,error)))=>complete(JobStatus(status,attempt,error))
       case Success(None)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("Job status lookup failed",error);complete(StatusCodes.InternalServerError)
      }
     }
    }
   }

  Http().newServerAt(host,port).bind(route).onComplete{
   case Success(binding)=>system.log.info("SBase server online at {}",binding.localAddress)
   case Failure(error)=>system.log.error("Failed to bind SBase HTTP server",error);system.terminate()
  }
 }
}
