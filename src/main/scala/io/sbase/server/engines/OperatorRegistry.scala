package io.sbase.server.engines
import org.apache.spark.sql.SparkSession
import org.apache.mahout.math.DenseVector
import spray.json._
import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.time.Duration

final class OperatorRegistry {
 private lazy val spark=SparkSession.builder().appName("SBase-Workflow-Operator")
  .master(sys.env.getOrElse("SPARK_MASTER","local[*]"))
  .config("spark.ui.enabled","false")
  .config("spark.driver.host",sys.env.getOrElse("SPARK_DRIVER_HOST","127.0.0.1")).getOrCreate()
 private val http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

 def execute(definition:String):JsValue = {
  val root=definition.parseJson.asJsObject
  val nodes=root.fields.getOrElse("nodes",JsArray()).convertTo[Vector[JsValue]]
  val outputs=nodes.map { value =>
   val node=value.asJsObject
   val id=node.fields.getOrElse("id",JsString("unnamed")).convertTo[String]
   val op=node.fields.getOrElse("operator",JsString("")).convertTo[String]
   val config=node.fields.getOrElse("config",JsObject()).asJsObject
   val output=op match {
    case "mahout-vector-sum" =>
     val values=config.fields.getOrElse("values",JsArray()).convertTo[Vector[Double]]
     val vector=new DenseVector(values.toArray)
     JsObject("sum"->JsNumber(vector.zSum()),"count"->JsNumber(values.size))
    case "spark-sql" =>
     val query=config.fields.getOrElse("query",JsString("SELECT 1 AS value")).convertTo[String]
     JsObject("rows"->JsArray(spark.sql(query).toJSON.collect().toVector.map(JsString(_))))
    case "flink-submit" =>
      val base = sys.env.getOrElse("FLINK_REST_URL", "http://localhost:8082")
      val jarId = config.fields.getOrElse("jarId", throw new IllegalArgumentException("flink-submit requires config.jarId")).convertTo[String]
      val payload = JsObject(
        "entryClass" -> config.fields.getOrElse("entryClass", JsString("")),
        "parallelism" -> config.fields.getOrElse("parallelism", JsNumber(1)),
        "programArgs" -> config.fields.getOrElse("programArgs", JsString(""))
      ).compactPrint
      val request = HttpRequest.newBuilder(URI.create(base.stripSuffix("/") + "/jars/" + jarId + "/run"))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload)).build()
      val response = http.send(request, HttpResponse.BodyHandlers.ofString())
      if (response.statusCode() / 100 != 2)
        throw new IllegalStateException("Flink job submission failed with HTTP " + response.statusCode() + ": " + response.body())
      response.body().parseJson
    case "flink-jobs-overview" =>
     val base=sys.env.getOrElse("FLINK_REST_URL","http://localhost:8082")
     val request=HttpRequest.newBuilder(URI.create(base.stripSuffix("/")+"/jobs/overview"))
      .timeout(Duration.ofSeconds(15)).GET().build()
     val response=http.send(request,HttpResponse.BodyHandlers.ofString())
     if(response.statusCode()/100!=2) throw new IllegalStateException("Flink REST HTTP "+response.statusCode())
     response.body().parseJson
    case other => throw new IllegalArgumentException("Unsupported workflow operator: "+other)
   }
   JsObject("nodeId"->JsString(id),"operator"->JsString(op),"output"->output)
  }
  JsObject("nodes"->JsArray(outputs))
 }
 def stop():Unit=if(spark!=null)spark.stop()
}
