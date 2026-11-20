package io.sbase.server.engines

import org.apache.spark.sql.SparkSession
import io.sbase.server.catalog.CatalogService
import org.apache.mahout.math.DenseVector
import spray.json._
import spray.json.DefaultJsonProtocol._
import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.time.Duration
import scala.collection.mutable

final class OperatorRegistry(catalogService:Option[CatalogService]=None) {
 private val supported=Set("dataset-values","dataset-rows","mahout-vector-sum","mahout-cosine-similarity","mahout-euclidean-distance","mahout-dot-product","mahout-manhattan-distance","mahout-normalize","mahout-top-k","cassandra-query","spark-sql","flink-submit","flink-jobs-overview","calcite-sql")
 private lazy val spark=SparkSession.builder().appName("SBase-Workflow-Operator")
  .master(sys.env.getOrElse("SPARK_MASTER","local[*]"))
  .config("spark.ui.enabled","false")
  .config("spark.driver.host",sys.env.getOrElse("SPARK_DRIVER_HOST","127.0.0.1")).getOrCreate()
 private val calcite=new CalciteSqlEngine()
 private val http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

 def sparkSession:SparkSession = spark
 def availableOperators:Vector[String] = supported.toVector.sorted
 def validate(definition:String):Unit = orderedNodes(definition)

 def operatorCatalog:java.util.List[java.util.Map[String,Object]] = {
  import scala.jdk.CollectionConverters._
  val definitions=Vector(
   descriptor("dataset-values","data","Inline numeric dataset source","values",Seq("values"),Seq.empty,"Emits a numeric values array"),
   descriptor("dataset-rows","data","Inline tabular dataset source","rows",Seq("rows"),Seq.empty,"Emits JSON records that can be registered as Spark SQL temporary views"),
   descriptor("mahout-vector-sum","mining","Mahout vector reduction","sum",Seq.empty,Seq("values"),"Computes vector sum and element count"),
   descriptor("mahout-cosine-similarity","mining","Mahout cosine similarity","cosineSimilarity",Seq("left","right"),Seq.empty,"Compares two equal-length numeric vectors"),
   descriptor("mahout-euclidean-distance","mining","Mahout Euclidean distance","euclideanDistance",Seq("left","right"),Seq.empty,"Computes L2 distance between numeric vectors"),
   descriptor("mahout-dot-product","mining","Mahout vector dot product","dotProduct",Seq("left","right"),Seq.empty,"Computes the inner product of equal-length vectors"),
   descriptor("mahout-manhattan-distance","mining","Mahout Manhattan distance","manhattanDistance",Seq("left","right"),Seq.empty,"Computes L1 distance between equal-length vectors"),
   descriptor("mahout-normalize","mining","Mahout L2 vector normalization","values",Seq("values"),Seq.empty,"Returns a unit-length vector; zero vectors remain zero"),
   descriptor("mahout-top-k","mining","Mahout top-K vector coordinates","items",Seq("values","k"),Seq.empty,"Returns the largest K vector coordinates in descending value order"),
   descriptor("cassandra-query","source","Cassandra CQL source","rows",Seq("cql"),Seq("limit"),"Executes a bounded read-only CQL query against the configured Cassandra catalog"),
   descriptor("calcite-sql","query","Embedded Calcite SQL over workflow-local JSON tables","rows",Seq("query","tables"),Seq.empty,"Executes relational SQL against configured tables"),
   descriptor("spark-sql","compute","Spark SQL execution","rows",Seq("query"),Seq.empty,"Executes Spark SQL using the configured Spark session"),
   descriptor("flink-submit","streaming","Submit an uploaded Flink JAR","jobId",Seq("jarId"),Seq("entryClass","parallelism","programArgs"),"Submits a Flink application through the Flink REST API"),
   descriptor("flink-jobs-overview","streaming","Inspect Flink jobs","jobs",Seq.empty,Seq.empty,"Returns Flink job overview")
  )
  definitions.map(_.asJava).asJava
 }

 def operatorDefinition(name:String):java.util.Optional[java.util.Map[String,Object]] =
  operatorCatalog.stream().filter(value=>value.get("name")==name).findFirst()

 def validateDetailed(definition:String):java.util.Map[String,Object] = {
  import scala.jdk.CollectionConverters._
  val root=definition.parseJson.asJsObject
  val nodes=root.fields.getOrElse("nodes",throw new IllegalArgumentException("Workflow must contain a nodes array")).convertTo[Vector[JsValue]]
  val edges=root.fields.get("edges").map(_.convertTo[Vector[JsValue]]).getOrElse(Vector.empty)
  val order=orderedNodes(definition).map(_.asJsObject.fields("id").convertTo[String])
  Map[String,Object](
   "valid"->java.lang.Boolean.TRUE,
   "nodeCount"->Int.box(nodes.size),
   "edgeCount"->Int.box(edges.size),
   "executionOrder"->order.asJava,
   "operators"->nodes.map(_.asJsObject.fields("operator").convertTo[String]).distinct.sorted.asJava
  ).asJava
 }

 private def descriptor(name:String,category:String,description:String,output:String,
                        required:Seq[String],optional:Seq[String],details:String):Map[String,Object] = {
  import scala.jdk.CollectionConverters._
  Map[String,Object](
   "name"->name,"category"->category,"description"->description,"output"->output,
   "requiredConfig"->required.asJava,"optionalConfig"->optional.asJava,"details"->details
  )
 }

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

 def execute(definition:String):JsValue = execute(definition,"{}")

 /** Resolve explicit {"$parameter":"name"} references from a JSON object supplied at invocation time. */
 def execute(definition:String,parametersJson:String):JsValue = {
  val parameters=parametersJson.parseJson.asJsObject.fields
  val resolved=resolveParameterRefs(definition.parseJson,parameters)
  val root=resolved.asJsObject
  val edges=root.fields.get("edges").map(_.convertTo[Vector[JsValue]]).getOrElse(Vector.empty)
  val incoming=edges.groupBy(_.asJsObject.fields("target").convertTo[String])
    .view.mapValues(_.map(_.asJsObject.fields("source").convertTo[String])).toMap
  val outputsById=mutable.Map.empty[String,JsValue]
  val outputs=mutable.ArrayBuffer.empty[JsValue]
  orderedNodes(resolved.compactPrint).foreach { value =>
   val node=value.asJsObject
   val id=node.fields("id").convertTo[String]
   val op=node.fields("operator").convertTo[String]
   val config=node.fields.getOrElse("config",JsObject()).asJsObject
   val upstream=incoming.getOrElse(id,Vector.empty).flatMap(outputsById.get)
   val output=op match {
    case "dataset-values" =>
     val values=config.fields.getOrElse("values",JsArray()).convertTo[Vector[Double]]
     JsObject("values"->JsArray(values.map(JsNumber(_))))
    case "dataset-rows" =>
     val rows=config.fields.getOrElse("rows",throw new IllegalArgumentException("dataset-rows requires config.rows")).convertTo[Vector[JsValue]]
     require(rows.forall(_.isInstanceOf[JsObject]),"dataset-rows accepts only JSON objects")
     JsObject("rows"->JsArray(rows))
    case "mahout-vector-sum" =>
     val configured=config.fields.get("values").map(_.convertTo[Vector[Double]])
     val inherited:Vector[Double]=upstream.flatMap(_.asJsObject.fields.get("values").map(_.convertTo[Vector[Double]])).flatten
     val values:Vector[Double]=configured.getOrElse(inherited)
     val vector=new DenseVector(values.toArray)
     JsObject("sum"->JsNumber(vector.zSum()),"count"->JsNumber(values.size))
    case "mahout-cosine-similarity" =>
     val leftValues=config.fields.getOrElse("left",throw new IllegalArgumentException("left vector is required")).convertTo[Vector[Double]]
     val rightValues=config.fields.getOrElse("right",throw new IllegalArgumentException("right vector is required")).convertTo[Vector[Double]]
     require(leftValues.nonEmpty && leftValues.size==rightValues.size,"Cosine vectors must have equal non-zero dimensions")
     val left=new DenseVector(leftValues.toArray);val right=new DenseVector(rightValues.toArray)
     val denominator=left.norm(2.0)*right.norm(2.0)
     val similarity=if(denominator==0.0) 0.0 else left.dot(right)/denominator
     JsObject("cosineSimilarity"->JsNumber(similarity))
    case "mahout-euclidean-distance" =>
     val leftValues=config.fields.getOrElse("left",throw new IllegalArgumentException("left vector is required")).convertTo[Vector[Double]]
     val rightValues=config.fields.getOrElse("right",throw new IllegalArgumentException("right vector is required")).convertTo[Vector[Double]]
     require(leftValues.nonEmpty && leftValues.size==rightValues.size,"Distance vectors must have equal non-zero dimensions")
     val distance=new DenseVector(leftValues.toArray).minus(new DenseVector(rightValues.toArray)).norm(2.0)
     JsObject("euclideanDistance"->JsNumber(distance))
    case "mahout-dot-product" =>
     val leftValues=config.fields.getOrElse("left",throw new IllegalArgumentException("left vector is required")).convertTo[Vector[Double]]
     val rightValues=config.fields.getOrElse("right",throw new IllegalArgumentException("right vector is required")).convertTo[Vector[Double]]
     require(leftValues.nonEmpty && leftValues.size==rightValues.size,"Dot-product vectors must have equal non-zero dimensions")
     val product=new DenseVector(leftValues.toArray).dot(new DenseVector(rightValues.toArray))
     JsObject("dotProduct"->JsNumber(product),"dimensions"->JsNumber(leftValues.size))
    case "mahout-manhattan-distance" =>
     val leftValues=config.fields.getOrElse("left",throw new IllegalArgumentException("left vector is required")).convertTo[Vector[Double]]
     val rightValues=config.fields.getOrElse("right",throw new IllegalArgumentException("right vector is required")).convertTo[Vector[Double]]
     require(leftValues.nonEmpty && leftValues.size==rightValues.size,"Manhattan vectors must have equal non-zero dimensions")
     val distance=leftValues.zip(rightValues).map { case (left,right)=>math.abs(left-right) }.sum
     JsObject("manhattanDistance"->JsNumber(distance),"dimensions"->JsNumber(leftValues.size))
    case "mahout-normalize" =>
     val values=config.fields.getOrElse("values",throw new IllegalArgumentException("values vector is required")).convertTo[Vector[Double]]
     val vector=new DenseVector(values.toArray)
     val norm=vector.norm(2.0)
     val normalized=if(norm==0.0) values else values.map(_/norm)
     JsObject("values"->JsArray(normalized.map(JsNumber(_))),"norm"->JsNumber(norm),"zeroVector"->JsBoolean(norm==0.0))
    case "mahout-top-k" =>
     val values=config.fields.getOrElse("values",throw new IllegalArgumentException("values vector is required")).convertTo[Vector[Double]]
     val k=config.fields.getOrElse("k",throw new IllegalArgumentException("k is required")).convertTo[Int]
     require(k>0 && k<=10000,"k must be between 1 and 10000")
     val selected=values.zipWithIndex.sortBy { case (value,index)=>(-value,index) }.take(k)
     JsObject("items"->JsArray(selected.map { case (value,index)=>JsObject("index"->JsNumber(index),"value"->JsNumber(value)) }))
    case "cassandra-query" =>
     val service=catalogService.getOrElse(throw new IllegalStateException("Cassandra workflow source is not configured"))
     val cql=config.fields.getOrElse("cql",throw new IllegalArgumentException("cassandra-query requires config.cql")).convertTo[String]
     val limit=config.fields.get("limit").map(_.convertTo[Int]).getOrElse(500)
     val result=service.queryCassandra(cql,limit)
     new ObjectMapper().writeValueAsString(result).parseJson
    case "spark-sql" =>
     val query=config.fields.getOrElse("query",JsString("SELECT 1 AS value")).convertTo[String]
     val inputTable=config.fields.get("inputTable").map(_.convertTo[String])
     inputTable.foreach { tableName =>
      require(tableName.matches("[A-Za-z_][A-Za-z0-9_]*"),"Invalid Spark input table name")
      val sourceRows=upstream.flatMap(_.asJsObject.fields.get("rows").map(_.convertTo[Vector[JsValue]])).headOption
      sourceRows.foreach { rows =>
       val jsonRows=rows.map {
        case JsString(json)=>json
        case other=>other.compactPrint
       }
       val frame=spark.read.json(spark.sparkContext.parallelize(jsonRows))
       frame.createOrReplaceTempView(tableName)
      }
     }
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


 private def resolveParameterRefs(value:JsValue,parameters:Map[String,JsValue]):JsValue = value match {
  case obj:JsObject if obj.fields.contains("$parameter") =>
   if(obj.fields.size!=1) throw new IllegalArgumentException("A parameter reference must contain only the $parameter key")
   val name=obj.fields("$parameter").convertTo[String]
   parameters.getOrElse(name,throw new IllegalArgumentException("Missing workflow invocation parameter: "+name))
  case obj:JsObject =>
   JsObject(obj.fields.map { case (key,nested)=>key->resolveParameterRefs(nested,parameters) })
  case JsArray(values) =>
   JsArray(values.map(value=>resolveParameterRefs(value,parameters)))
  case other=>other
 }

 def autoMl(request:String):JsValue = {
  val mapper=new com.fasterxml.jackson.databind.ObjectMapper()
  val input=mapper.readValue(request,classOf[java.util.Map[String,Object]])
  val result=new io.sbase.server.automl.AutoMlService(spark).train(input)
  mapper.writeValueAsString(result).parseJson
 }

 def stop():Unit=if(spark!=null)spark.stop()
}
