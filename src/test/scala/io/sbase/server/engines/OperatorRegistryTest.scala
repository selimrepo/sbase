package io.sbase.server.engines

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class OperatorRegistryTest {
  @Test
  def mahoutVectorSumExecutes(): Unit = {
    val registry = new OperatorRegistry()
    val result = registry.execute("""{"nodes":[{"id":"sum","operator":"mahout-vector-sum","config":{"values":[1.5,2.5,4.0]}}]}""")
    assertTrue(result.compactPrint.contains("\"sum\":8.0"))
    assertTrue(result.compactPrint.contains("\"count\":3"))
  }

  @Test
  def dagExecutesInTopologicalOrder(): Unit = {
    val registry = new OperatorRegistry()
    val definition = """{"nodes":[
      {"id":"second","operator":"mahout-vector-sum","config":{"values":[2]}},
      {"id":"first","operator":"mahout-vector-sum","config":{"values":[1]}}
    ],"edges":[{"source":"first","target":"second"}]}"""
    val output = registry.execute(definition).compactPrint
    assertTrue(output.indexOf("\"nodeId\":\"first\"") < output.indexOf("\"nodeId\":\"second\""))
  }

  @Test
  def connectedDatasetFeedsMahoutOperator(): Unit = {
    val registry = new OperatorRegistry()
    val definition = """{"nodes":[
      {"id":"source","operator":"dataset-values","config":{"values":[2,4,8]}},
      {"id":"sum","operator":"mahout-vector-sum","config":{}}
    ],"edges":[{"source":"source","target":"sum"}]}"""
    val output = registry.execute(definition).compactPrint
    assertTrue(output.contains("\"sum\":14.0"))
  }

  @Test
  def cyclicWorkflowIsRejected(): Unit = {
    val registry = new OperatorRegistry()
    val cycle = """{"nodes":[
      {"id":"a","operator":"mahout-vector-sum","config":{"values":[1]}},
      {"id":"b","operator":"mahout-vector-sum","config":{"values":[2]}}
    ],"edges":[{"source":"a","target":"b"},{"source":"b","target":"a"}]}"""
    assertThrows(classOf[IllegalArgumentException], () => registry.validate(cycle))
  }

  @Test
  def mahoutCosineSimilarityExecutes(): Unit = {
    val registry = new OperatorRegistry()
    val result = registry.execute("""{"nodes":[{"id":"cos","operator":"mahout-cosine-similarity","config":{"left":[1,0],"right":[1,1]}}]}""").compactPrint
    assertTrue(result.contains("cosineSimilarity"))
    assertTrue(result.contains("0.707"))
  }

  @Test
  def mahoutEuclideanDistanceExecutes(): Unit = {
    val registry = new OperatorRegistry()
    val result = registry.execute("""{"nodes":[{"id":"distance","operator":"mahout-euclidean-distance","config":{"left":[0,0],"right":[3,4]}}]}""").compactPrint
    assertTrue(result.contains("euclideanDistance"))
    assertTrue(result.contains("5.0"))
  }
  @Test
  def sparkSqlOperatorRunsAQuery(): Unit = {
    val registry = new OperatorRegistry()
    try {
      val result = registry.execute("""{"nodes":[{"id":"sql","operator":"spark-sql","config":{"query":"SELECT 2 + 3 AS answer"}}]}""")
      assertTrue(result.compactPrint.contains("answer"))
      assertTrue(result.compactPrint.contains("5"))
    } finally registry.stop()
  }

  @Test
  def deploymentParametersResolveTypedConfigReferences(): Unit = {
    val registry = new OperatorRegistry()
    val definition = """{"nodes":[{"id":"sum","operator":"mahout-vector-sum","config":{"values":{"$parameter":"numbers"}}}]}"""
    val result = registry.execute(definition, """{"numbers":[2.0,3.5,4.5]}""").compactPrint
    assertTrue(result.contains("\"sum\":10.0"))
    assertThrows(classOf[IllegalArgumentException], () => registry.execute(definition, """{"other":[1,2]}"""))
  }


  @Test
  def mahoutDotProductExecutes(): Unit = {
    val result = new OperatorRegistry().execute("""{"nodes":[{"id":"dot","operator":"mahout-dot-product","config":{"left":[1,2,3],"right":[4,5,6]}}]}""").compactPrint
    assertTrue(result.contains("\"dotProduct\":32.0"))
  }

  @Test
  def mahoutManhattanDistanceExecutes(): Unit = {
    val result = new OperatorRegistry().execute("""{"nodes":[{"id":"l1","operator":"mahout-manhattan-distance","config":{"left":[1,2],"right":[4,6]}}]}""").compactPrint
    assertTrue(result.contains("\"manhattanDistance\":7.0"))
  }

  @Test
  def mahoutNormalizationHandlesZeroVectors(): Unit = {
    val result = new OperatorRegistry().execute("""{"nodes":[{"id":"normalize","operator":"mahout-normalize","config":{"values":[0,0]}}]}""").compactPrint
    assertTrue(result.contains("\"zeroVector\":true"))
  }

  @Test
  def mahoutTopKIsStableAndDescending(): Unit = {
    val result = new OperatorRegistry().execute("""{"nodes":[{"id":"top","operator":"mahout-top-k","config":{"values":[3,9,9,1],"k":2}}]}""").compactPrint
    assertTrue(result.contains("\"index\":1"))
    assertTrue(result.contains("\"index\":2"))
    assertTrue(result.indexOf("\"index\":1") < result.indexOf("\"index\":2"))
  }
}
