
name := "ingestion3"
organization := "dpla"
version := "0.0.1"
scalaVersion := "2.13.13"
val SPARK_VERSION = "3.5.1"

ThisBuild / Test / parallelExecution := false

assembly / assemblyMergeStrategy := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "org.apache.spark" %% "spark-avro" % SPARK_VERSION,
  "org.apache.spark" %% "spark-hadoop-cloud" % SPARK_VERSION,
  "org.eclipse.rdf4j" % "rdf4j" % "2.2",
  "org.eclipse.rdf4j" % "rdf4j-model" % "2.2",
  "org.jsoup" % "jsoup" % "1.17.2",
  "org.apache.commons" % "commons-text" % "1.11.0",
  "org.rogach" % "scallop_2.13" % "5.1.0",
  "org.scalamock" %% "scalamock" % "5.1.0" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "3.5.0_1.4.7" % "test",
  "com.typesafe" % "config" % "1.3.1",
  "com.opencsv" % "opencsv" % "3.7",
  "net.lingala.zip4j" % "zip4j" % "2.11.5",
  "javax.mail" % "mail" % "1.4.7",
  "org.apache.ant" % "ant" % "1.8.0",
  "org.apache.ant" % "ant-compress" % "1.5"
)

 resolvers += "SparkPackages" at "https://repos.spark-packages.org/"
