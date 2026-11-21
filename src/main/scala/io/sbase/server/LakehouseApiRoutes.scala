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
      path("lakehouse" / "namespaces" / Segment / "tables" / Segment / "snapshots") { (namespace,table) =>
        optionalHeaderValueByName("Authorization") { auth => get { run(auth)(s => service.get.snapshots(s,namespace,table)) } }
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
