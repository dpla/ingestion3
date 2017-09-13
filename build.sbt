
lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    name := "ingestion3",
    version := "0.0.1",
    scalaVersion := "2.11.8",
    Defaults.itSettings,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.1" % "it,test",
      "org.apache.spark" %% "spark-core" % "2.0.1" exclude("org.scalatest", "scalatest_2.11"),
      "org.apache.spark" %% "spark-sql" % "2.0.1" exclude("org.scalatest", "scalatest_2.11"),
      "org.apache.ant" % "ant" % "1.10.1",
      "com.databricks" %% "spark-avro" % "3.2.0",
      "org.json4s" %% "json4s-core" % "3.2.11" % "provided",
      "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
      "org.eclipse.rdf4j" % "rdf4j" % "2.2",
      "org.eclipse.rdf4j" % "rdf4j-model" % "2.2",
      "org.eclipse.rdf4j" % "rdf4j-rio-api" % "2.2",
      "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % "2.2",
      // For Elasticsearch, see https://www.elastic.co/guide/en/elasticsearch/hadoop/current/install.html
      "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.3.2", // Spark 2.0+, Scala 2.11+
      // CdlHarvester depends
      "org.apache.httpcomponents" % "httpclient" % "4.5.2",
      "org.apache.httpcomponents" % "fluent-hc" % "4.5.2",
      // Enricher dependencies
      // TODO: reconcile with httpclient above.
      "org.jsoup" % "jsoup" % "1.10.2",
      "org.scalaj" %% "scalaj-http" % "2.3.0",
      "org.eclipse.rdf4j" % "rdf4j-model" % "2.2",
      "org.eclipse.rdf4j" % "rdf4j-rio-api" % "2.2",
      "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % "2.2",
      "org.scalaj" % "scalaj-http_2.11" % "2.3.0",
      "org.rogach" % "scallop_2.11" % "3.0.3",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % "test",
      "com.typesafe" % "config" % "1.3.1"
    )
  )
