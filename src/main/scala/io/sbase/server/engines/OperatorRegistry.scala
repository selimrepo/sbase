package io.sbase.server.engines

import org.apache.spark.sql.SparkSession
import org.apache.mahout.math.DenseVector
import spray.json._
import spray.json.DefaultJsonProtocol._
import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.time.Duration
import scala.collection.mutable

final class OperatorRegistry {
 private val supported=Set("dataset-values","mahout-vector-sum","spark-sql","flink-submit","flink-jobs-overview")
 private lazy val spark=SparkSession.builder().appName("SBase-Workflow-Operator")
  .master(sys.env.getOrElse("SPARK_MASTER","local[*]"))
  .config("spark.ui.enabled","false")
  .config("spark.driver.host",sys.env.getOrElse("SPARK_DRIVER_HOST","127.0.0.1")).getOrCreate()
 private val http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

 def validate(definition:String):Unit = orderedNodes(definition)

 private def orderedNodes(definition:String):Vector[JsValue] = {
  val root=definition.parseJson.asJsObject
  val nodes=root.fields.getOrElse("nodes",throw new IllegalArgumentException("Workflow must contain a nodes array")).convertTo[Vector[JsValue]]
  val ids=nodes.map(n=>n.asJsObject.fields.getOrElse("id",throw new IllegalArgumentException("Every node needs an id")).convertTo[String])
  if(ids.distinct.size!=ids.size) throw new IllegalArgumentException("Workflow node IDs must be unique")
  nodes.foreach { n =>
   val op=n.asJsObject.fields.getOrElse("operator",JsString("")).convertTo[String]
   if(!supported.contains(op)) throw new IllegalArgumentException("Unsupported workflow operator: "+op)
  }
  val byId=nodes.map(n=>n.asJsObject.fields("id").convertTo[String]->n).toMap
  val edges=root.fields.get("edges").map(_.convertTo[Vector[JsValue]]).getOrElse(Vector.empty)
  val outgoing=mutable.Map(ids.map(_->Vector.empty[String]):_*)
  val degree=mutable.Map(ids.map(id=>id->0):_*)
  edges.foreach { edge =>
   val fields=edge.asJsObject.fields
   val source=fields("source").convertTo[String]; val target=fields("target").convertTo[String]
   if(!byId.contains(source)|| !byId.contains(target)) throw new IllegalArgumentException("Workflow edge references an unknown node")
   outgoing(source)=outgoing(source):+target
   degree(target)=degree(target)+1
  }
  val ready=mutable.Queue(ids.filter(degree(_)==0):_*)
  val ordered=mutable.ArrayBuffer.empty[JsValue]
  while(ready.nonEmpty) {
   val id=ready.dequeue();ordered+=byId(id)
   outgoing(id).foreach { target=>degree(target)=degree(target)-1;if(degree(target)==0)ready.enqueue(target) }
  }
  if(ordered.size!=nodes.size) throw new IllegalArgumentException("Workflow graph contains a cycle")
  ordered.toVector
 }

 def execute(definition:String):JsValue = {
  val root=definition.parseJson.asJsObject
  val edges=root.fields.get("edges").map(_.convertTo[Vector[JsValue]]).getOrElse(Vector.empty)
  val incoming=edges.groupBy(_.asJsObject.fields("target").convertTo[String])
    .view.mapValues(_.map(_.asJsObject.fields("source").convertTo[String])).toMap
  val outputsById=mutable.Map.empty[String,JsValue]
  val outputs=mutable.ArrayBuffer.empty[JsValue]
  orderedNodes(definition).foreach { value =>
   val node=value.asJsObject
   val id=node.fields("id").convertTo[String]
   val op=node.fields("operator").convertTo[String]
   val config=node.fields.getOrElse("config",JsObject()).asJsObject
   val upstream=incoming.getOrElse(id,Vector.empty).flatMap(outputsById.get)
   val output=op match {
    case "dataset-values" =>
     val values=config.fields.getOrElse("values",JsArray()).convertTo[Vector[Double]]
     JsObject("values"->JsArray(values.map(JsNumber(_))))
    case "mahout-vector-sum" =>
     val configured=config.fields.get("values").map(_.convertTo[Vector[Double]])
     val inherited:Vector[Double]=upstream.flatMap(_.asJsObject.fields.get("values").map(_.convertTo[Vector[Double]])).flatten
     val values:Vector[Double]=configured.getOrElse(inherited)
     val vector=new DenseVector(values.toArray)
     JsObject("sum"->JsNumber(vector.zSum()),"count"->JsNumber(values.size))
    case "spark-sql" =>
     val query=config.fields.getOrElse("query",JsString("SELECT 1 AS value")).convertTo[String]
     JsObject("rows"->JsArray(spark.sql(query).toJSON.collect().toVector.map(JsString(_))))
    case "flink-submit" =>
     val base=sys.env.getOrElse("FLINK_REST_URL","http://localhost:8082")
     val jarId=config.fields.getOrElse("jarId",throw new IllegalArgumentException("flink-submit requires config.jarId")).convertTo[String]
     val payload=JsObject(
      "entryClass"->config.fields.getOrElse("entryClass",JsString("")),
      "parallelism"->config.fields.getOrElse("parallelism",JsNumber(1)),
      "programArgsList"->config.fields.getOrElse("programArgs",JsString(""))
     ).compactPrint
     val request=HttpRequest.newBuilder(URI.create(base.stripSuffix("/")+"/jars/"+jarId+"/run"))
      .timeout(Duration.ofSeconds(30)).header("Content-Type","application/json")
      .POST(HttpRequest.BodyPublishers.ofString(payload)).build()
     val response=http.send(request,HttpResponse.BodyHandlers.ofString())
     if(response.statusCode()/100!=2) throw new IllegalStateException("Flink submission failed HTTP "+response.statusCode()+": "+response.body())
     response.body().parseJson
    case "flink-jobs-overview" =>
     val base=sys.env.getOrElse("FLINK_REST_URL","http://localhost:8082")
     val request=HttpRequest.newBuilder(URI.create(base.stripSuffix("/")+"/jobs/overview")).timeout(Duration.ofSeconds(15)).GET().build()
     val response=http.send(request,HttpResponse.BodyHandlers.ofString())
     if(response.statusCode()/100!=2) throw new IllegalStateException("Flink REST HTTP "+response.statusCode())
     response.body().parseJson
   }
   outputsById += id -> output
   outputs += JsObject("nodeId"->JsString(id),"operator"->JsString(op),"output"->output)
  }
  JsObject("nodes"->JsArray(outputs.toVector))
 }
 def stop():Unit=if(spark!=null)spark.stop()
}
