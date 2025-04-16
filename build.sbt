
name := "ingestion3"
organization := "dpla"
version := "0.0.1"
scalaVersion := "2.13.15"
fork  := true
outputStrategy := Some(StdoutOutput)
val SPARK_VERSION = "3.5.5"

ThisBuild / Test / parallelExecution := false

assembly / assemblyMergeStrategy := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "org.apache.spark" %% "spark-avro" % SPARK_VERSION,
  "org.apache.spark" %% "spark-hadoop-cloud" % SPARK_VERSION exclude("stax", "stax-api"),
  //"org.eclipse.rdf4j" % "rdf4j" % "5.1.0",
  "org.eclipse.rdf4j" % "rdf4j-model" % "5.1.0",
  "org.jsoup" % "jsoup" % "1.18.3",
  "org.apache.commons" % "commons-text" % "1.13.0",
  "org.rogach" %% "scallop" % "5.2.0",
  "org.scalamock" %% "scalamock" % "6.0.0" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "3.5.3_2.0.1" % "test",
  "com.typesafe" % "config" % "1.4.3",
  "com.opencsv" % "opencsv" % "5.9",
  "net.lingala.zip4j" % "zip4j" % "2.11.5",
  "javax.mail" % "mail" % "1.4.7",
  "org.apache.ant" % "ant" % "1.10.15",
  "org.apache.ant" % "ant-compress" % "1.5"
)

 resolvers += "SparkPackages" at "https://repos.spark-packages.org/"
