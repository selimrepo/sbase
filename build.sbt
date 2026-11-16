ThisBuild / organization := "io.sbase"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

lazy val pekkoVersion = "1.1.3"
lazy val pekkoHttpVersion = "1.1.0"

lazy val root = (project in file(".")).enablePlugins(JavaAppPackaging)
  .settings(
    name := "sbase-server",
    Compile / mainClass := Some("io.sbase.server.Main"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHttpVersion,
      "com.typesafe" % "config" % "1.4.3",
      "com.zaxxer" % "HikariCP" % "6.2.1",
      "org.postgresql" % "postgresql" % "42.7.7",
      "org.flywaydb" % "flyway-core" % "11.8.2",
      "org.flywaydb" % "flyway-database-postgresql" % "11.8.2",
      "org.apache.spark" %% "spark-sql" % "4.0.1" % Provided,
      "org.apache.spark" %% "spark-mllib" % "4.0.1" % Provided,
      "org.apache.mahout" % "mahout-math" % "14.1",
      "org.apache.flink" % "flink-streaming-java" % "2.1.0" % Provided,
      "com.datastax.oss" % "java-driver-core" % "4.18.1",
      "org.apache.solr" % "solr-solrj" % "9.8.1",
      "org.apache.geode" % "geode-core" % "1.15.2" % Provided,
      "ch.qos.logback" % "logback-classic" % "1.5.18",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpVersion % Test,
      "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test
    ),
    Test / parallelExecution := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Wconf:cat=unused-imports:s"),
    Compile / doc / sources := Seq.empty
  )
