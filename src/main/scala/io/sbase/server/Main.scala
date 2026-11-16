package io.sbase.server

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import spray.json.DefaultJsonProtocol._
import com.typesafe.config.ConfigFactory
import io.sbase.server.jobs.{JobDispatcher,JobRepository,WorkerSharding}
import io.sbase.server.engines.{OperatorRegistry,FederatedQueryPlanner,CalciteSqlEngine,FederatedQueryExecutor}
import io.sbase.server.catalog.CatalogService
import io.sbase.server.automl.AutoMlLifecycleService
import com.fasterxml.jackson.databind.ObjectMapper
import scala.concurrent.{ExecutionContext,Future}
import scala.util.{Failure,Success}
import java.util.UUID
import scala.jdk.CollectionConverters._

object Main {
 final case class ApiInfo(name:String,version:String,status:String)
 final case class Health(status:String,service:String)
 final case class WorkflowCreate(name:String,definition:JsValue)
 final case class WorkflowCreated(workspaceId:String,workflowId:String)
 final case class JobSubmit(idempotencyKey:String,maxAttempts:Int=5)
 final case class JobAccepted(jobId:String)
 final case class JobStatus(status:String,attempt:Int,error:Option[String],result:Option[String])
 final case class PgQuery(sql:String,parameters:Vector[JsValue]=Vector.empty,limit:Int=1000)
 final case class CassandraQuery(cql:String,limit:Int=500)
 final case class SolrQueryRequest(collection:String,q:String="*:*",limit:Int=100)

 def ApiInfoName():String="SBase Data Science Server"

 def main(args:Array[String]):Unit={
  val clusterHost=sys.env.getOrElse("SBASE_CLUSTER_HOST","127.0.0.1")
  val clusterPort=sys.env.get("SBASE_CLUSTER_PORT").flatMap(_.toIntOption).getOrElse(25520)
  val bindHost=sys.env.getOrElse("SBASE_CLUSTER_BIND_HOST",clusterHost)
  val bindPort=sys.env.get("SBASE_CLUSTER_BIND_PORT").flatMap(_.toIntOption).getOrElse(clusterPort)
  val seedNodes=sys.env.get("SBASE_CLUSTER_SEEDS")
    .map(_.split(",").map(_.trim).filter(_.nonEmpty).map(s => "\"" + s + "\"").mkString(","))
    .getOrElse("\"pekko://sbase@127.0.0.1:25520\"")
  val clusterConfigText =
    "pekko.remote.artery.canonical.hostname=\"" + clusterHost + "\"\n" +
    "pekko.remote.artery.canonical.port=" + clusterPort + "\n" +
    "pekko.remote.artery.bind.hostname=\"" + bindHost + "\"\n" +
    "pekko.remote.artery.bind.port=" + bindPort + "\n" +
    "pekko.cluster.seed-nodes=[" + seedNodes + "]\n"
  val clusterConfig=ConfigFactory.parseString(clusterConfigText).withFallback(ConfigFactory.load())
  implicit val system:ActorSystem[Nothing]=ActorSystem[Nothing](Behaviors.empty[Nothing],"sbase",clusterConfig)
  implicit val ec:ExecutionContext=system.executionContext
  implicit val apiInfoFormat=jsonFormat3(ApiInfo.apply)
  implicit val healthFormat=jsonFormat2(Health.apply)
  implicit val workflowCreateFormat=jsonFormat2(WorkflowCreate.apply)
  implicit val workflowCreatedFormat=jsonFormat2(WorkflowCreated.apply)
  implicit val jobSubmitFormat=jsonFormat2(JobSubmit.apply)
  implicit val jobAcceptedFormat=jsonFormat1(JobAccepted.apply)
  implicit val jobStatusFormat=jsonFormat4(JobStatus.apply)
  implicit val pgQueryFormat=jsonFormat3(PgQuery.apply)
  implicit val cassandraQueryFormat=jsonFormat2(CassandraQuery.apply)
  implicit val solrQueryFormat=jsonFormat3(SolrQueryRequest.apply)

  val repository=JobRepository.fromEnvironment()
  repository.migrate()
  val operators=new OperatorRegistry()
  val catalogs=new CatalogService()
  val mapper=new ObjectMapper()
  val federated=new FederatedQueryExecutor(catalogs,new CalciteSqlEngine(),sys.env.get("SBASE_FEDERATED_SCAN_PARALLELISM").flatMap(_.toIntOption).getOrElse(8))
  val autoMl=new AutoMlLifecycleService(operators.sparkSession)
  val workerRegion=WorkerSharding.start(system,repository,operators)
  new JobDispatcher(system,repository,workerRegion).start()

  val host=sys.env.getOrElse("SBASE_HTTP_HOST","0.0.0.0")
  val port=sys.env.get("SBASE_HTTP_PORT").flatMap(_.toIntOption).getOrElse(8080)

  val route=
   path("health"){get{complete(Health("UP","sbase-server"))}} ~
   pathPrefix("api"/"v1"){
    path("capabilities"){get{complete(JsObject(
      "catalogs"->JsArray(JsString("postgresql"),JsString("cassandra"),JsString("solr")),
      "queryEngines"->JsArray(JsString("calcite-sql"),JsString("federated-calcite"),JsString("spark-sql")),
      "compute"->JsArray(JsString("mahout"),JsString("spark-ml-automl"),JsString("flink")),
      "apiVersion"->JsString("v1")
    ))}} ~
    path("operators"){get{complete(JsArray(operators.availableOperators.map(JsString(_))))}} ~
    path("catalogs"){get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(catalogs.catalogs()).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Catalog discovery failed",error);complete(StatusCodes.InternalServerError)
      }
    }} ~
    path("catalogs"/"postgresql"/"query"){post{entity(as[PgQuery]){request=>
      val params: java.util.List[Object] = request.parameters.map {
       case JsString(v)=>v
       case JsNumber(v)=>v.bigDecimal
       case JsBoolean(v)=>java.lang.Boolean.valueOf(v)
       case JsNull=>null
       case other=>other.compactPrint
      }.map(_.asInstanceOf[Object]).asJava
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(catalogs.queryPostgres(request.sql,params,request.limit)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("PostgreSQL query rejected/failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("query failed"))))
      }
    }}} ~
    path("catalogs"/"cassandra"/"query"){post{entity(as[CassandraQuery]){request=>
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(catalogs.queryCassandra(request.cql,request.limit)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Cassandra query rejected/failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("query failed"))))
      }
    }}} ~
    path("catalogs"/"solr"/"query"){post{entity(as[SolrQueryRequest]){request=>
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(catalogs.querySolr(request.collection,request.q,request.limit)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Solr query rejected/failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("query failed"))))
      }
    }}} ~
    path("query"/"plan"){post{entity(as[JsObject]){request=>
      val sql=request.fields.getOrElse("sql",JsString("")).convertTo[String]
      val catalogJson=request.fields.getOrElse("catalog",JsObject()).compactPrint
      onComplete(Future(scala.concurrent.blocking{
       val stats=mapper.readValue(catalogJson,classOf[java.util.Map[String,java.util.Map[String,Object]]])
       mapper.writeValueAsString(new FederatedQueryPlanner().plan(sql,stats)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Federated query planning failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("planning failed"))))
      }
    }}} ~
    path("query"/"execute"){post{entity(as[JsObject]){request=>
      val sql=request.fields.getOrElse("sql",JsString("")).convertTo[String]
      val sourceJson=request.fields.getOrElse("sources",JsObject()).compactPrint
      val limit=request.fields.get("limit").map(_.convertTo[Int]).getOrElse(1000)
      onComplete(Future(scala.concurrent.blocking{
       val sources=mapper.readValue(sourceJson,classOf[java.util.Map[String,java.util.Map[String,Object]]])
       mapper.writeValueAsString(federated.execute(sql,sources,limit)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Federated query execution failed",error);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("query execution failed"))))
      }
    }}} ~
    path("automl"/"train"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(autoMl.train(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("AutoML training failed",error);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("training failed"))))
      }
    }}} ~
    path("automl"/"experiments"){get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.experiments(100)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("AutoML experiment listing failed",error);complete(StatusCodes.InternalServerError)
      }
    }} ~
    path("models"/JavaUUID){modelId=>
      get{onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.model(modelId)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("Model lookup failed",error);complete(StatusCodes.InternalServerError)
      }} ~
      post{entity(as[JsObject]){request=>
       val rows=mapper.readValue(request.fields.getOrElse("rows",JsArray()).compactPrint,
         mapper.getTypeFactory.constructCollectionType(classOf[java.util.List[_]],classOf[java.util.Map[_,_]]))
       onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.predict(modelId,rows.asInstanceOf[java.util.List[java.util.Map[String,Object]]].asScala.toList.asJava)).parseJson))){
        case Success(value)=>complete(value)
        case Failure(error)=>system.log.error("Model prediction failed",error);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("prediction failed"))))
       }
      }}
    }}
   } ~
   pathPrefix("api"/"v1"){
    pathEndOrSingleSlash{get{complete(ApiInfo(ApiInfoName(),"0.2.0-SNAPSHOT","WORKFLOW_ENGINE"))}} ~
    path("ready"){get{onComplete(Future(scala.concurrent.blocking(repository.isHealthy))){
      case Success(true)=>complete(Health("UP","postgres-and-pekko-sharding"))
      case Success(false)=>complete(StatusCodes.ServiceUnavailable->Health("NOT_READY","postgres"))
      case Failure(error)=>system.log.error("Readiness check failed",error);complete(StatusCodes.ServiceUnavailable->Health("NOT_READY","postgres"))
     }}} ~
    path("workflows"){
     post{
      entity(as[WorkflowCreate]){request=>
       val name=request.name.trim
       if(name.isEmpty) complete(StatusCodes.BadRequest)
       else {
        val definition=request.definition.compactPrint
        try {
         operators.validate(definition)
         val future=Future(scala.concurrent.blocking(repository.createWorkflow(name,definition)))
         onComplete(future){
          case Success((workspaceId,workflowId))=>complete(StatusCodes.Created->WorkflowCreated(workspaceId.toString,workflowId.toString))
          case Failure(error)=>system.log.error("Workflow creation failed",error);complete(StatusCodes.InternalServerError)
         }
        } catch {
         case error:IllegalArgumentException =>
          system.log.warn("Rejected invalid workflow definition: {}",error.getMessage)
          complete(StatusCodes.BadRequest)
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
       case Success(Some((status,attempt,error,result)))=>complete(JobStatus(status,attempt,error,result))
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
