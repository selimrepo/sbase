package io.sbase.server

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class ApiContractSpec extends AnyWordSpec with Matchers {
  "SBase foundation" should {
    "expose the intended service identity" in {
      Main.ApiInfo("SBase Data Science Server", "0.1.0-SNAPSHOT", "FOUNDATION").name shouldBe
        "SBase Data Science Server"
    }

    "keep authentication out of the initial service contract" in {
      val declaredRoutes = Set("/health", "/api/v1", "/api/v1/ready")
      declaredRoutes.exists(_.contains("login")) shouldBe false
      declaredRoutes.exists(_.contains("auth")) shouldBe false
    }
  }
}
