package io.sbase.server

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import com.typesafe.config.ConfigFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main {
  final case class ApiInfo(name: String, version: String, status: String)
  final case class Health(status: String, service: String)

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "sbase")
    implicit val ec: ExecutionContext = system.executionContext
    implicit val apiInfoFormat = jsonFormat3(ApiInfo.apply)
    implicit val healthFormat = jsonFormat2(Health.apply)

    val host = sys.env.getOrElse("SBASE_HTTP_HOST", "0.0.0.0")
    val port = sys.env.get("SBASE_HTTP_PORT").flatMap(_.toIntOption).getOrElse(8080)

    val route =
      path("health") {
        get {
          complete(Health("UP", "sbase-server"))
        }
      } ~
      pathPrefix("api" / "v1") {
        pathEndOrSingleSlash {
          get {
            complete(ApiInfo("SBase Data Science Server", "0.1.0-SNAPSHOT", "FOUNDATION"))
          }
        } ~
        path("ready") {
          get {
            complete(StatusCodes.ServiceUnavailable -> Health("NOT_READY", "database-and-workers-not-yet-wired"))
          }
        }
      }

    Http().newServerAt(host, port).bind(route).onComplete {
      case Success(binding) =>
        system.log.info("SBase HTTP server online at {}", binding.localAddress)
      case Failure(error) =>
        system.log.error("Failed to bind SBase HTTP server", error)
        system.terminate()
    }
  }
}
