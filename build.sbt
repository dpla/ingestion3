
name := "ingestion3"
organization := "dpla"
version := "0.0.1"
scalaVersion := "2.11.8"

parallelExecution in Test := false

assembly / assemblyMergeStrategy := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  // "org.scalatest" %% "scalatest" % "3.0.1" % "it,test",

  /**
    * The following dependencies enable S3 file writes:
    *   spark 2.3.1
    *   spark-avro 4.0.0
    *   aws-java-sdk 1.7.4
    *   hadoop-aws 2.7.6
    */
  "org.apache.spark" %% "spark-core" % "2.3.1" exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % "2.3.1" exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-mllib" % "2.3.1" exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.ant" % "ant" % "1.10.1",
  "com.databricks" %% "spark-avro" % "4.0.0",
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
  //"org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % "test",
  "org.scalamock" %% "scalamock" % "4.0.0" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.8.0" % "test",
  "com.typesafe" % "config" % "1.3.1",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.6",
  "com.squareup.okhttp3" % "okhttp" % "3.8.0",
  "com.opencsv" % "opencsv" % "3.7",
  "databricks" % "spark-corenlp" % "0.3.1-s_2.11",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.1" classifier "models",
  // specify hadoop-mapreduce-client-core version to avoid Stopwatch/guava dependency conflicts
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.8.1"
)

 resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"
