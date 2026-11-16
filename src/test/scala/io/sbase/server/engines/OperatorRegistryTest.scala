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
  def cyclicWorkflowIsRejected(): Unit = {
    val registry = new OperatorRegistry()
    val cycle = """{"nodes":[
      {"id":"a","operator":"mahout-vector-sum","config":{"values":[1]}},
      {"id":"b","operator":"mahout-vector-sum","config":{"values":[2]}}
    ],"edges":[{"source":"a","target":"b"},{"source":"b","target":"a"}]}"""
    assertThrows(classOf[IllegalArgumentException], () => registry.validate(cycle))
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
}
