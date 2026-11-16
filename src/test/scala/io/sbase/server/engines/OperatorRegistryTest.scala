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
  def unsupportedOperatorFailsFast(): Unit = {
    val registry = new OperatorRegistry()
    assertThrows(classOf[IllegalArgumentException], () =>
      registry.execute("""{"nodes":[{"id":"bad","operator":"missing-operator","config":{}}]}"""))
  }
}
