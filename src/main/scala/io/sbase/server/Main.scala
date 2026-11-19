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
import io.sbase.server.automl.{AutoMlLifecycleService,AutoMlJobQueue,AutoMlScienceService,AutoMlMonitoringService}
import io.sbase.server.runtime.PlatformRuntimeService
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.`type`.TypeReference
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
  val federated=new FederatedQueryExecutor(catalogs,operators.sparkSession,sys.env.get("SBASE_FEDERATED_SCAN_PARALLELISM").flatMap(_.toIntOption).getOrElse(8))
  val autoMl=new AutoMlLifecycleService(operators.sparkSession)
  val autoMlJobs=new AutoMlJobQueue(autoMl,operators.sparkSession,sys.env.get("SBASE_AUTOML_WORKERS").flatMap(_.toIntOption).getOrElse(2),sys.env.get("SBASE_AUTOML_QUEUE_CAPACITY").flatMap(_.toIntOption).getOrElse(16))
  val autoMlScience=new AutoMlScienceService(operators.sparkSession)
  val scientificMining=new io.sbase.server.automl.ScientificMiningService()
  val scientificResearch=new io.sbase.server.automl.ScientificResearchService()
  val autoMlStatisticalDrift=new AutoMlMonitoringService()
  val runtime=new PlatformRuntimeService(operators.sparkSession)
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
      "scientificAutoMl"->JsArray(JsString("dataset-profiling"),JsString("cross-validation"),JsString("model-cards"),JsString("fairness-diagnostics"),JsString("psi-drift"),JsString("kolmogorov-smirnov"),JsString("feature-association"),JsString("fdr-feature-selection"),JsString("welch-hypothesis-testing"),JsString("mann-whitney-u"),JsString("kruskal-wallis"),JsString("one-way-anova"),JsString("rolling-mad-anomaly-detection"),JsString("propensity-score-ipw"),JsString("experiment-power-analysis"),JsString("version-pinned-workflow-deployments"),JsString("rest-workflow-services"),JsString("deployment-concurrency-limits"),JsString("workflow-audit-trail")),
      "apiVersion"->JsString("v1")
    ))}} ~
    path("components"){get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(runtime.components()).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Component discovery failed",error);complete(StatusCodes.InternalServerError)
      }
    }} ~
    path("operators"){get{complete(JsArray(operators.availableOperators.map(JsString(_))))}} ~
    path("operators"/"catalog"){get{complete(mapper.writeValueAsString(operators.operatorCatalog).parseJson)}} ~
    path("operators"/Segment){name=>get{
      val definition=operators.operatorDefinition(name)
      if(definition.isPresent) complete(mapper.writeValueAsString(definition.get()).parseJson)
      else complete(StatusCodes.NotFound->JsObject("error"->JsString("Unknown workflow operator")))
    }} ~
    path("engines"){get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(runtime.engines()).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Engine status inspection failed",error);complete(StatusCodes.InternalServerError)
      }
    }} ~
    path("engines"/"spark"/"sql"){post{entity(as[JsObject]){request=>
      val sql=request.fields.get("sql").map(_.convertTo[String]).getOrElse("")
      val limit=request.fields.get("limit").map(_.convertTo[Int]).getOrElse(1000)
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(runtime.sparkSql(sql,limit)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Spark SQL request rejected/failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("Spark SQL failed"))))
      }
    }}} ~
    path("engines"/"flink"/Segment){resource=>get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(runtime.flinkGet(resource)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Flink resource request failed: {}",error.getMessage);complete(StatusCodes.BadGateway->JsObject("error"->JsString(Option(error.getMessage).getOrElse("Flink unavailable"))))
      }
    }} ~
    path("engines"/"flink"/"jobs"/Segment){jobId=>get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(runtime.flinkJob(jobId)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Flink job lookup failed: {}",error.getMessage);complete(StatusCodes.BadGateway->JsObject("error"->JsString(Option(error.getMessage).getOrElse("Flink job lookup failed"))))
      }
    }} ~
    path("engines"/"flink"/"jars"/Segment/"run"){jarId=>post{entity(as[JsObject]){request=>
      val config=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(runtime.flinkSubmit(jarId,config)).parseJson))){
       case Success(value)=>complete(StatusCodes.Accepted->value)
       case Failure(error)=>system.log.warn("Flink JAR submission failed: {}",error.getMessage);complete(StatusCodes.BadGateway->JsObject("error"->JsString(Option(error.getMessage).getOrElse("Flink submission failed"))))
      }
    }}} ~
    path("catalogs"/"postgresql"/"columns"){get{
      parameter("schema".as[String].withDefault("public")){schema=>
       parameter("table".as[String]){table=>
        onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(catalogs.postgresColumns(schema,table)).parseJson))){
         case Success(value)=>complete(value)
         case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
         case Failure(error)=>system.log.warn("PostgreSQL schema inspection failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("schema inspection failed"))))
        }
       }
      }
    }} ~
    path("automl"/"metrics"){get{complete(JsObject(
      "classification"->JsArray(JsString("accuracy"),JsString("f1"),JsString("weightedPrecision"),JsString("weightedRecall")),
      "regression"->JsArray(JsString("rmse"),JsString("mae"),JsString("mse"),JsString("r2"),JsString("var")),
      "time_series"->JsArray(JsString("rmse"),JsString("mae"),JsString("mse"),JsString("r2")),
      "selectionNote"->JsString("Metric direction is handled by the AutoML trainer; compare metrics only within the same task and validation protocol.")
    ))}} ~
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
    path("automl"/"science"/"associations"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificMining.associations(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Scientific association mining failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("association analysis failed"))))
      }
    }}} ~
    path("automl"/"science"/"feature-selection"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificMining.selectFeatures(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Scientific feature selection failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("feature selection failed"))))
      }
    }}} ~
    path("automl"/"science"/"compare-groups"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificMining.compareGroups(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Scientific group comparison failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("group comparison failed"))))
      }
    }}} ~
    path("automl"/"science"/"mann-whitney"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificResearch.mannWhitneyU(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Mann-Whitney analysis failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("Mann-Whitney analysis failed"))))
      }
    }}} ~
    path("automl"/"science"/"kruskal-wallis"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificResearch.kruskalWallis(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Kruskal-Wallis analysis failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("Kruskal-Wallis analysis failed"))))
      }
    }}} ~
    path("automl"/"science"/"anova"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificResearch.oneWayAnova(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("One-way ANOVA failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("ANOVA failed"))))
      }
    }}} ~
    path("automl"/"science"/"time-series"/"anomalies"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificResearch.detectTimeSeriesAnomalies(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Time-series anomaly detection failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("anomaly detection failed"))))
      }
    }}} ~
    path("automl"/"science"/"causal-effect"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificResearch.estimateCausalEffect(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Causal effect estimation failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("causal effect estimation failed"))))
      }
    }}} ~
    path("automl"/"science"/"experiment-design"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(scientificResearch.designContinuousExperiment(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Experiment design failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("experiment design failed"))))
      }
    }}} ~
    path("automl"/"data"/"profile"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(autoMlScience.profile(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("AutoML data profiling failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("profiling failed"))))
      }
    }}} ~
    path("automl"/"monitoring"/"statistical-drift"){post{entity(as[JsObject]){request=>
      onComplete(Future(scala.concurrent.blocking{
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       mapper.writeValueAsString(autoMlStatisticalDrift.analyze(input)).parseJson
      })){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.warn("Statistical feature drift analysis failed: {}",error.getMessage);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("drift analysis failed"))))
      }
    }}} ~
    path("automl"/"monitoring"/"statistical-runs"){get{
      parameter("limit".as[Int].withDefault(50)){limit=>
       onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMlStatisticalDrift.runs(null,limit)).parseJson))){
        case Success(value)=>complete(value)
        case Failure(error)=>system.log.error("Statistical drift history failed",error);complete(StatusCodes.InternalServerError)
       }
      }
    }} ~
    path("models"/JavaUUID/"statistical-drift"){modelId=>get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMlStatisticalDrift.runs(modelId,100)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Model statistical drift history failed",error);complete(StatusCodes.InternalServerError)
      }
    }} ~
    path("automl"/"jobs"){post{entity(as[JsObject]){request=>
      try {
       val input=mapper.readValue(request.compactPrint,classOf[java.util.Map[String,Object]])
       complete(StatusCodes.Accepted->mapper.writeValueAsString(autoMlJobs.submit(input)).parseJson)
      } catch {
       case error:IllegalStateException=>complete(StatusCodes.ServiceUnavailable->JsObject("error"->JsString(Option(error.getMessage).getOrElse("training queue unavailable"))))
       case error:Exception=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("invalid request"))))
      }
    }}} ~
    path("automl"/"jobs"/JavaUUID){jobId=>
      get{
       try complete(mapper.writeValueAsString(autoMlJobs.status(jobId)).parseJson)
       catch { case _:NoSuchElementException=>complete(StatusCodes.NotFound) }
      } ~
      post {
       try complete(mapper.writeValueAsString(autoMlJobs.cancel(jobId)).parseJson)
       catch { case _:NoSuchElementException=>complete(StatusCodes.NotFound) }
      }
    } ~
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
    path("automl"/"experiments"/JavaUUID){experimentId=>
      get{onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.experiment(experimentId)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("AutoML experiment lookup failed",error);complete(StatusCodes.InternalServerError)
      }}
    } ~
    path("models"){get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.models(100)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Model registry listing failed",error);complete(StatusCodes.InternalServerError)
      }
    }} ~
    path("models"/"aliases"){get{
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.aliases()).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Model alias listing failed",error);complete(StatusCodes.InternalServerError)
      }
    }} ~
    path("models"/"aliases"/Segment){alias=>
      get{onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.alias(alias)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("Model alias lookup failed",error);complete(StatusCodes.InternalServerError)
      }} ~
      put{entity(as[JsObject]){request=>
       val modelId=UUID.fromString(request.fields.getOrElse("modelId",JsString("")).convertTo[String])
       onComplete(Future(scala.concurrent.blocking{autoMl.promote(alias,modelId);JsObject("alias"->JsString(alias),"modelId"->JsString(modelId.toString))})){
        case Success(value)=>complete(value)
        case Failure(error)=>system.log.error("Model promotion failed",error);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("promotion failed"))))
       }
      }}
    } ~
    path("models"/"aliases"/Segment/"rollback"){alias=>
      post{onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.rollbackAlias(alias)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("Model alias rollback failed",error);complete(StatusCodes.Conflict->JsObject("error"->JsString(Option(error.getMessage).getOrElse("rollback failed"))))
      }}
    } ~ path("models"/JavaUUID/"card"){modelId=>
      get{onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.model(modelId).get("modelCard")).parseJson))){
       case Success(value)=>complete(value)
       case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("Model card lookup failed",error);complete(StatusCodes.InternalServerError)
      }}
    } ~
    path("models"/JavaUUID/"monitoring"){modelId=>
      get{parameter("limit".as[Int].withDefault(50)){limit=>
       onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.monitoringHistory(modelId,limit)).parseJson))){
        case Success(value)=>complete(value)
        case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
        case Failure(error)=>system.log.error("Model monitoring history failed",error);complete(StatusCodes.InternalServerError)
       }
      }} ~
      post{entity(as[JsObject]){request=>
       try {
        val rows=mapper.readValue(request.fields.getOrElse("rows",JsArray()).compactPrint,
          new TypeReference[java.util.List[java.util.Map[String,Object]]]() {})
        val data=rows.asScala.map(row=>row.asScala.toMap.asJava).asJava
        val policy=mapper.readValue(request.fields.getOrElse("policy",JsObject()).compactPrint,
          new TypeReference[java.util.Map[String,Object]]() {})
        val retrainingRequest=request.fields.get("retrainingRequest").map(value=>
          mapper.readValue(value.compactPrint,new TypeReference[java.util.Map[String,Object]]() {})).orNull
        onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(
          autoMl.monitor(modelId,data,policy.asScala.toMap.asJava,
            Option(retrainingRequest).map(_.asScala.toMap.asJava).orNull)).parseJson))){
         case Success(value)=>complete(value)
         case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
         case Failure(error)=>system.log.error("Model drift monitoring failed",error);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("monitoring failed"))))
        }
       } catch {case error:Exception=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("invalid monitoring request"))))}
      }}
    } ~
    path("models"/JavaUUID/"archive"){modelId=>
      post{onComplete(Future(scala.concurrent.blocking{autoMl.archive(modelId);JsObject("status"->JsString("ARCHIVED"))})){
       case Success(value)=>complete(value)
       case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("Model archival failed",error);complete(StatusCodes.BadRequest)
      }}
    } ~
    path("models"/JavaUUID){modelId=>
      get{onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.model(modelId)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("Model lookup failed",error);complete(StatusCodes.InternalServerError)
      }} ~
      post{entity(as[JsObject]){request=>
       val rows=mapper.readValue(request.fields.getOrElse("rows",JsArray()).compactPrint,
         new TypeReference[java.util.List[java.util.Map[String,Object]]]() {})
       onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(autoMl.predict(modelId,rows)).parseJson))){
        case Success(value)=>complete(value)
        case Failure(error)=>system.log.error("Model prediction failed",error);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("prediction failed"))))
       }
      }}
    }
   } ~
   pathPrefix("api"/"v1"){
    pathEndOrSingleSlash{get{complete(ApiInfo(ApiInfoName(),"0.2.0-SNAPSHOT","WORKFLOW_ENGINE"))}} ~
    path("ready"){get{onComplete(Future(scala.concurrent.blocking(repository.isHealthy))){
      case Success(true)=>complete(Health("UP","postgres-and-pekko-sharding"))
      case Success(false)=>complete(StatusCodes.ServiceUnavailable->Health("NOT_READY","postgres"))
      case Failure(error)=>system.log.error("Readiness check failed",error);complete(StatusCodes.ServiceUnavailable->Health("NOT_READY","postgres"))
     }}} ~
    path("workflows"){get{
     parameter("limit".as[Int].withDefault(100)){limit=>
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(repository.workflows(limit)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Workflow listing failed",error);complete(StatusCodes.InternalServerError)
      }
     }
    }} ~
    path("workflows"/"validate"){post{entity(as[JsObject]){request=>
     val definition=request.fields.get("definition").map(_.compactPrint).getOrElse(request.compactPrint)
     try complete(mapper.writeValueAsString(operators.validateDetailed(definition)).parseJson)
     catch {case error:IllegalArgumentException=>complete(StatusCodes.BadRequest->JsObject("valid"->JsBoolean(false),"error"->JsString(Option(error.getMessage).getOrElse("invalid workflow"))))}
    }}} ~
    path("workflows"/JavaUUID){workflowId=>get{
     onComplete(Future(scala.concurrent.blocking(repository.workflow(workflowId)))){
      case Success(Some(value))=>complete(mapper.writeValueAsString(value).parseJson)
      case Success(None)=>complete(StatusCodes.NotFound)
      case Failure(error)=>system.log.error("Workflow lookup failed",error);complete(StatusCodes.InternalServerError)
     }
    }} ~
    path("workflows"/JavaUUID/"versions"/IntNumber){(workflowId,version)=>get{
     onComplete(Future(scala.concurrent.blocking(repository.workflowVersion(workflowId,version)))){
      case Success(Some(value))=>complete(mapper.writeValueAsString(value).parseJson)
      case Success(None)=>complete(StatusCodes.NotFound)
      case Failure(error)=>system.log.error("Workflow version lookup failed",error);complete(StatusCodes.InternalServerError)
     }
    }} ~

    (path("workflows"/JavaUUID/"deployments"){workflowId=>
     get{
      parameter("limit".as[Int].withDefault(100)){limit=>
       onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(repository.deployments(Some(workflowId),limit)).parseJson))){
        case Success(value)=>complete(value)
        case Failure(error)=>system.log.error("Workflow deployment listing failed",error);complete(StatusCodes.InternalServerError)
       }
      }
     } ~
     post{entity(as[JsObject]){request=>
      try {
       val slug=request.fields.getOrElse("slug",JsString("")).convertTo[String]
       val version=request.fields.get("version").map(_.convertTo[Int]).getOrElse(0)
       val concurrency=request.fields.get("maxConcurrentRuns").map(_.convertTo[Int]).getOrElse(5)
       val attempts=request.fields.get("maxAttempts").map(_.convertTo[Int]).getOrElse(5)
       onComplete(Future(scala.concurrent.blocking(repository.publishDeployment(workflowId,slug,version,concurrency,attempts)))){
        case Success(id)=>complete(StatusCodes.Created->JsObject("deploymentId"->JsString(id.toString),"workflowId"->JsString(workflowId.toString),"slug"->JsString(slug),"version"->JsNumber(if(version>0)version else repository.workflow(workflowId).flatMap(v=>Option(v("activeVersion")).map(_.toString.toInt)).getOrElse(0))))
        case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound->JsObject("error"->JsString("Workflow or requested version not found")))
        case Failure(error)=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("deployment publication failed"))))
       }
      } catch {case error:Exception=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("invalid deployment request"))))}
     }}
    }) ~
    path("workflows"/JavaUUID/"audit"){workflowId=>get{
     parameter("limit".as[Int].withDefault(100)){limit=>
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(repository.workflowAudit(workflowId,limit)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Workflow audit lookup failed",error);complete(StatusCodes.InternalServerError)
      }
     }
    }} ~
    path("deployments"){get{
     parameter("limit".as[Int].withDefault(100)){limit=>
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(repository.deployments(None,limit)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Deployment listing failed",error);complete(StatusCodes.InternalServerError)
      }
     }
    }} ~
    path("deployments"/Segment){slug=>get{
     onComplete(Future(scala.concurrent.blocking(repository.deployment(slug)))){
      case Success(Some(value))=>complete(mapper.writeValueAsString(value).parseJson)
      case Success(None)=>complete(StatusCodes.NotFound)
      case Failure(error)=>system.log.error("Deployment lookup failed",error);complete(StatusCodes.InternalServerError)
     }
    }} ~
    path("deployments"/Segment/"pause"){slug=>post{
     onComplete(Future(scala.concurrent.blocking(repository.setDeploymentEnabled(slug,false)))){
      case Success(true)=>complete(JsObject("slug"->JsString(slug),"enabled"->JsBoolean(false)))
      case Success(false)=>complete(StatusCodes.NotFound)
      case Failure(error)=>system.log.error("Deployment pause failed",error);complete(StatusCodes.InternalServerError)
     }
    }} ~
    path("deployments"/Segment/"resume"){slug=>post{
     onComplete(Future(scala.concurrent.blocking(repository.setDeploymentEnabled(slug,true)))){
      case Success(true)=>complete(JsObject("slug"->JsString(slug),"enabled"->JsBoolean(true)))
      case Success(false)=>complete(StatusCodes.NotFound)
      case Failure(error)=>system.log.error("Deployment resume failed",error);complete(StatusCodes.InternalServerError)
     }
    }} ~
    path("deployments"/Segment/"runs"){slug=>post{entity(as[JsObject]){request=>
     try {
      val key=request.fields.getOrElse("idempotencyKey",JsString("")).convertTo[String]
      val attempts=request.fields.get("maxAttempts").map(_.convertTo[Int]).getOrElse(5)
      val parameters=request.fields.get("parameters") match {
       case None | Some(JsNull)=>"{}"
       case Some(value:JsObject)=>value.compactPrint
       case Some(_)=>throw new IllegalArgumentException("parameters must be a JSON object")
      }
      onComplete(Future(scala.concurrent.blocking(repository.invokeDeployment(slug,key,attempts,parameters)))){
       case Success((jobId,version))=>complete(StatusCodes.Accepted->JsObject("jobId"->JsString(jobId.toString),"workflowVersion"->JsNumber(version),"status"->JsString("QUEUED")))
       case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound->JsObject("error"->JsString("Deployment not found")))
       case Failure(error) if Option(error.getMessage).exists(_.contains("concurrency limit"))=>complete(StatusCodes.TooManyRequests->JsObject("error"->JsString(error.getMessage)))
       case Failure(error) if Option(error.getMessage).exists(_.contains("paused"))=>complete(StatusCodes.Conflict->JsObject("error"->JsString(error.getMessage)))
       case Failure(error)=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("deployment invocation failed"))))
      }
     } catch {case error:Exception=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("invalid invocation request"))))}
    }}} ~
    path("jobs"){get{
     parameter("limit".as[Int].withDefault(100)){limit=>
      onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(repository.jobs(limit)).parseJson))){
       case Success(value)=>complete(value)
       case Failure(error)=>system.log.error("Job listing failed",error);complete(StatusCodes.InternalServerError)
      }
     }
    }} ~
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
    path("workflows"/JavaUUID/"versions"){workflowId=>
     get {
      onComplete(Future(scala.concurrent.blocking(repository.workflowVersions(workflowId)))) {
       case Success(versions)=>
        val payload=JsArray(versions.map{case (version,checksum,definition,created)=>
         JsObject("version"->JsNumber(version),"checksum"->JsString(checksum),"definition"->definition.parseJson,"createdAt"->JsString(created.toString))
        }.toVector)
        complete(payload)
       case Failure(error)=>system.log.error("Workflow version listing failed",error);complete(StatusCodes.InternalServerError)
      }
     } ~
     post { entity(as[JsObject]){request=>
      try {
       val definition=request.fields.getOrElse("definition",JsObject()).compactPrint
       operators.validate(definition)
       val activate=request.fields.get("activate").exists(_.convertTo[Boolean])
       onComplete(Future(scala.concurrent.blocking(repository.createWorkflowVersion(workflowId,definition,activate)))) {
        case Success(version)=>complete(StatusCodes.Created->JsObject("workflowId"->JsString(workflowId.toString),"version"->JsNumber(version),"active"->JsBoolean(activate)))
        case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
        case Failure(error)=>system.log.error("Workflow version creation failed",error);complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("version creation failed"))))
       }
      } catch {case error:IllegalArgumentException=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("invalid workflow"))))}
     }}
    } ~
    path("workflows"/JavaUUID/"versions"/IntNumber/"activate"){(workflowId,version)=>
     put {
      onComplete(Future(scala.concurrent.blocking(repository.activateWorkflowVersion(workflowId,version)))) {
       case Success(true)=>complete(JsObject("workflowId"->JsString(workflowId.toString),"activeVersion"->JsNumber(version)))
       case Success(false)=>complete(StatusCodes.NotFound)
       case Failure(error)=>system.log.error("Workflow activation failed",error);complete(StatusCodes.InternalServerError)
      }
     }
    } ~
    path("workflows"/JavaUUID/"schedules"){workflowId=>
     get {
      onComplete(Future(scala.concurrent.blocking(repository.schedules(Some(workflowId))))) {
       case Success(items)=>complete(mapper.writeValueAsString(items).parseJson)
       case Failure(error)=>system.log.error("Workflow schedules listing failed",error);complete(StatusCodes.InternalServerError)
      }
     } ~
     post { entity(as[JsObject]){request=>
      try {
       val interval=request.fields.getOrElse("intervalSeconds",JsNumber(3600)).convertTo[Int]
       val attempts=request.fields.getOrElse("maxAttempts",JsNumber(5)).convertTo[Int]
       onComplete(Future(scala.concurrent.blocking(repository.createSchedule(workflowId,interval,attempts)))) {
        case Success(id)=>complete(StatusCodes.Created->JsObject("scheduleId"->JsString(id.toString),"workflowId"->JsString(workflowId.toString),"intervalSeconds"->JsNumber(interval)))
        case Failure(_:NoSuchElementException)=>complete(StatusCodes.NotFound)
        case Failure(error)=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("schedule creation failed"))))
       }
      } catch {case error:Exception=>complete(StatusCodes.BadRequest->JsObject("error"->JsString(Option(error.getMessage).getOrElse("invalid schedule"))))}
     }}
    } ~
    path("schedules"){get {
     onComplete(Future(scala.concurrent.blocking(repository.schedules(None)))) {
      case Success(items)=>complete(mapper.writeValueAsString(items).parseJson)
      case Failure(error)=>system.log.error("Schedule listing failed",error);complete(StatusCodes.InternalServerError)
     }
    }} ~
    path("schedules"/JavaUUID/"pause"){scheduleId=>post {
     onComplete(Future(scala.concurrent.blocking(repository.setScheduleEnabled(scheduleId,false)))) {
      case Success(true)=>complete(JsObject("scheduleId"->JsString(scheduleId.toString),"enabled"->JsBoolean(false)))
      case Success(false)=>complete(StatusCodes.NotFound)
      case Failure(error)=>system.log.error("Schedule pause failed",error);complete(StatusCodes.InternalServerError)
     }
    }} ~
    path("schedules"/JavaUUID/"resume"){scheduleId=>post {
     onComplete(Future(scala.concurrent.blocking(repository.setScheduleEnabled(scheduleId,true)))) {
      case Success(true)=>complete(JsObject("scheduleId"->JsString(scheduleId.toString),"enabled"->JsBoolean(true)))
      case Success(false)=>complete(StatusCodes.NotFound)
      case Failure(error)=>system.log.error("Schedule resume failed",error);complete(StatusCodes.InternalServerError)
     }
    }} ~
    path("workflows"/JavaUUID/"runs"){workflowId=>
     get {
      parameter("limit".as[Int].withDefault(100)){limit=>
       onComplete(Future(scala.concurrent.blocking(repository.workflowRuns(workflowId,limit)))) {
        case Success(items)=>complete(mapper.writeValueAsString(items).parseJson)
        case Failure(error)=>system.log.error("Workflow run history failed",error);complete(StatusCodes.InternalServerError)
       }
      }
     } ~
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
    path("jobs"/JavaUUID/"events"){jobId=>
     get {
      onComplete(Future(scala.concurrent.blocking(repository.jobEvents(jobId)))) {
       case Success(items)=>complete(mapper.writeValueAsString(items).parseJson)
       case Failure(error)=>system.log.error("Job event history failed",error);complete(StatusCodes.InternalServerError)
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
