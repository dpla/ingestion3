import sbt.Keys.libraryDependencies

name := "ingestion3"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
  "org.json4s" %% "json4s-core" % "3.2.11" % "provided",
  "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
  "com.twitter" %% "finagle-http" % "6.41.0",
  "org.eclipse.rdf4j" % "rdf4j" % "2.1.4",
  "org.elasticsearch" % "elasticsearch-hadoop" % "2.0.3"
    excludeAll(ExclusionRule("cascading", "cascading-hadoop"), ExclusionRule("cascading", "cascading-local")),
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  //  ApiHarvester depends
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "com.lambdaworks" %% "jacks" % "2.3.3",
//  Needed for SequenceFile writing
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.7.3",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
//  CdlMapping dependency
  "org.apache.jena" % "jena-tdb" % "3.1.0",
)
