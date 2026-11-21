package io.sbase.server

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.`type`.TypeReference
import io.sbase.server.lakehouse.LakehouseApiService
import org.apache.pekko.http.scaladsl.server.{Directives,Route}
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import scala.concurrent.{ExecutionContext,Future}
import scala.util.{Failure,Success}
import java.util.UUID
import scala.jdk.CollectionConverters._

/** Authenticated HTTP surface for the Iceberg catalog and SQL workspace. */
object LakehouseApiRoutes extends Directives {
  def route(service: Option[LakehouseApiService], mapper: ObjectMapper)(implicit ec: ExecutionContext): Route = {
    def unavailable: Route = complete(StatusCodes.ServiceUnavailable -> JsObject("error" -> JsString("Lakehouse JWT authentication is not configured; set SBASE_JWT_SECRET (32+ characters)")))
    def run(auth: Option[String])(f: LakehouseApiService.Session => Any): Route = service match {
      case None => unavailable
      case Some(api) =>
        try {
          val session=api.authenticate(auth.orNull)
          onComplete(Future(scala.concurrent.blocking(mapper.writeValueAsString(f(session))).parseJson)) {
            case Success(value) => complete(value)
            case Failure(e: SecurityException) => complete(StatusCodes.Forbidden -> JsObject("error" -> JsString(Option(e.getMessage).getOrElse("Access denied"))))
            case Failure(e: NoSuchElementException) => complete(StatusCodes.NotFound -> JsObject("error" -> JsString(Option(e.getMessage).getOrElse("Not found"))))
            case Failure(e: IllegalArgumentException) => complete(StatusCodes.BadRequest -> JsObject("error" -> JsString(Option(e.getMessage).getOrElse("Invalid request"))))
            case Failure(e) => complete(StatusCodes.BadGateway -> JsObject("error" -> JsString(Option(e.getMessage).getOrElse("Lakehouse operation failed"))))
          }
        } catch {
          case e: SecurityException => complete(StatusCodes.Unauthorized -> JsObject("error" -> JsString(Option(e.getMessage).getOrElse("Authentication failed"))))
          case e: IllegalArgumentException => complete(StatusCodes.Unauthorized -> JsObject("error" -> JsString("Invalid access token claims")))
        }
    }

    pathPrefix("api" / "v1") {
      path("lakehouse") {
        optionalHeaderValueByName("Authorization") { auth =>
          get { run(auth)(s => service.get.status(s)) }
        }
      } ~
      path("lakehouse" / "namespaces") {
        optionalHeaderValueByName("Authorization") { auth =>
          get { run(auth)(s => service.get.namespaces(s)) } ~
          post {
            entity(as[JsObject]) { body =>
              val name=body.fields.get("namespace").map(_.convertTo[String]).getOrElse("")
              run(auth)(s => service.get.createNamespace(s,name))
            }
          }
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables") { namespace =>
        optionalHeaderValueByName("Authorization") { auth =>
          get { run(auth)(s => service.get.tables(s,namespace)) }
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment) { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth =>
          get {
            parameter("limit".as[Int].withDefault(100)) { limit =>
              run(auth)(s => service.get.preview(s,namespace,table,limit))
            }
          }
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "write") { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth =>
          post {
            entity(as[JsObject]) { body =>
              val rows=mapper.readValue(body.fields.getOrElse("rows",JsArray()).compactPrint,
                new TypeReference[java.util.List[java.util.Map[String,Object]]]() {})
              val converted=rows.asScala.map(v=>v.asScala.toMap.asJava).asJava
              run(auth)(s => service.get.writeRows(s,namespace,table,converted))
            }
          }
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "schema") { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth => get { run(auth)(s => service.get.schema(s,namespace,table)) } }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "schema" / "evolve") { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth =>
          post { entity(as[JsObject]) { body =>
            val request=mapper.readValue(body.compactPrint,new TypeReference[java.util.Map[String,Object]]() {})
            run(auth)(s => service.get.evolveSchema(s,namespace,table,request))
          }}
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "snapshots") { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth => get { run(auth)(s => service.get.snapshots(s,namespace,table)) } }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "snapshots" / LongNumber / "rollback") { (namespace,table,snapshotId) =>
        optionalHeaderValueByName("Authorization") { auth => post { run(auth)(s => service.get.rollbackSnapshot(s,namespace,table,snapshotId)) } }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "compact") { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth => post { run(auth)(s => service.get.compact(s,namespace,table)) } }
      } ~
      path("lakehouse" / "namespaces" / Segment / "maintenance") { namespace =>
        optionalHeaderValueByName("Authorization") { auth =>
          get { run(auth)(s => service.get.maintenancePolicies(s,namespace)) } ~
          post { entity(as[JsObject]) { body =>
            val table=body.fields.get("table").map(_.convertTo[String]).getOrElse("")
            val operation=body.fields.get("operation").map(_.convertTo[String]).getOrElse("COMPACT")
            val interval=body.fields.get("intervalSeconds").map(_.convertTo[Int]).getOrElse(86400)
            val retention=body.fields.get("retentionDays").map(_.convertTo[Int]).getOrElse(7)
            val orphan=body.fields.get("orphanOlderThanHours").map(_.convertTo[Int]).getOrElse(72)
            run(auth)(s => service.get.createMaintenancePolicy(s,namespace,table,operation,interval,retention,orphan))
          }}
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "grants") { namespace =>
        optionalHeaderValueByName("Authorization") { auth =>
          get { run(auth)(s => service.get.grants(s,namespace)) } ~
          post { entity(as[JsObject]) { body =>
            val request=mapper.readValue(body.compactPrint,new TypeReference[java.util.Map[String,Object]]() {})
            run(auth)(s => service.get.grantNamespace(s,namespace,request))
          }}
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "grants" / "revoke") { namespace =>
        optionalHeaderValueByName("Authorization") { auth =>
          post { entity(as[JsObject]) { body =>
            val request=mapper.readValue(body.compactPrint,new TypeReference[java.util.Map[String,Object]]() {})
            run(auth)(s => service.get.revokeGrant(s,namespace,null,request))
          }}
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "grants") { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth =>
          post { entity(as[JsObject]) { body =>
            val request=mapper.readValue(body.compactPrint,new TypeReference[java.util.Map[String,Object]]() {})
            run(auth)(s => service.get.grantTable(s,namespace,table,request))
          }}
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "grants" / "revoke") { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth =>
          post { entity(as[JsObject]) { body =>
            val request=mapper.readValue(body.compactPrint,new TypeReference[java.util.Map[String,Object]]() {})
            run(auth)(s => service.get.revokeGrant(s,namespace,table,request))
          }}
        }
      } ~
      path("lakehouse" / "namespaces" / Segment / "lineage") { namespace =>
        optionalHeaderValueByName("Authorization") { auth =>
          get { parameter("limit".as[Int].withDefault(200)) { limit =>
            run(auth)(s => service.get.lineage(s,namespace,limit))
          }}
        }
      } ~
      path("lakehouse" / "runs" / JavaUUID / "dead-letter") { runId =>
        optionalHeaderValueByName("Authorization") { auth =>
          get { parameter("limit".as[Int].withDefault(200)) { limit =>
            run(auth)(s => service.get.deadLetters(s,runId,limit))
          }}
        }
      } ~
      path("lakehouse" / "runs" / JavaUUID / "dead-letter" / "claim") { runId =>
        optionalHeaderValueByName("Authorization") { auth =>
          post { entity(as[JsObject]) { body =>
            val limit=body.fields.get("limit").map(_.convertTo[Int]).getOrElse(100)
            run(auth)(s => service.get.claimDeadLetters(s,runId,limit))
          }}
        }
      } ~
      path("lakehouse" / "dead-letter" / JavaUUID / "finish") { deadLetterId =>
        optionalHeaderValueByName("Authorization") { auth =>
          post { entity(as[JsObject]) { body =>
            val resolved=body.fields.get("resolved").exists(_.convertTo[Boolean])
            run(auth)(s => service.get.finishDeadLetter(s,deadLetterId,resolved))
          }}
        }
      } ~
      path("lakehouse" / "sources" / JavaUUID / "cdc" / "offsets") { sourceId =>
        optionalHeaderValueByName("Authorization") { auth =>
          post { entity(as[JsObject]) { body =>
            val stream=body.fields.get("stream").map(_.convertTo[String]).getOrElse("")
            val partition=body.fields.get("partition").map(_.convertTo[String]).getOrElse("")
            val offset=body.fields.get("offset").map(_.compactPrint).getOrElse("{}")
            val generation=body.fields.get("expectedGeneration").map(_.convertTo[Long]).getOrElse(0L)
            run(auth)(s => service.get.commitCdcOffset(s,sourceId,stream,partition,offset,generation))
          }}
        }
      } ~
      path("lakehouse" / "pipelines") {
        optionalHeaderValueByName("Authorization") { auth =>
          get { run(auth)(s => service.get.pipelines(s)) } ~
          post { entity(as[JsObject]) { body =>
            val request=mapper.readValue(body.compactPrint,new TypeReference[java.util.Map[String,Object]]() {})
            run(auth)(s => service.get.createPipeline(s,request))
          }}
        }
      } ~
      path("lakehouse" / "pipelines" / JavaUUID / "runs") { pipelineId =>
        optionalHeaderValueByName("Authorization") { auth =>
          get { parameter("limit".as[Int].withDefault(50)) { limit =>
            run(auth)(s => service.get.pipelineRuns(s,pipelineId,limit))
          }} ~
          post { entity(as[JsObject]) { body =>
            val key=body.fields.get("idempotencyKey").map(_.convertTo[String]).getOrElse("")
            run(auth)(s => service.get.runPipeline(s,pipelineId,key))
          }}
        }
      } ~
      path("engines" / "spark" / "sql") {
        optionalHeaderValueByName("Authorization") { auth =>
          post {
            entity(as[JsObject]) { body =>
              val sql=body.fields.get("sql").map(_.convertTo[String]).getOrElse("")
              val limit=body.fields.get("limit").map(_.convertTo[Int]).getOrElse(1000)
              run(auth)(s => service.get.query(s,sql,limit))
            }
          }
        }
      } ~
      pathPrefix("lakehouse") {
        optionalHeaderValueByName("Authorization") { auth =>
          service match {
            case None => unavailable
            case Some(api) =>
              try { api.authenticate(auth.orNull); complete(StatusCodes.NotFound -> JsObject("error" -> JsString("Unknown Lakehouse endpoint"))) }
              catch { case e: SecurityException => complete(StatusCodes.Unauthorized -> JsObject("error" -> JsString(Option(e.getMessage).getOrElse("Authentication failed")))) }
          }
        }
      }
    }
  }
}
