// Enforce Java 11+ (required for java.net.http used in HttpUtils and UpdateInstitutions)
val _ = {
  val v = sys.props("java.specification.version")
  val tooOld = v == "1.8" || v == "8" ||
    (v.forall(c => c.isDigit) && scala.util
      .Try(v.toInt)
      .toOption
      .exists(_ < 11))
  if (tooOld)
    sys.error(
      "Java 11+ is required for build. Current version: " + v +
        ". Set JAVA_HOME to a JDK 11+ (e.g. in .env) and run 'source .env' then 'sbt assembly'."
    )
}

name := "ingestion3"
organization := "dpla"
version := "0.0.1"
scalaVersion := "2.13.15"
fork := true
outputStrategy := Some(StdoutOutput)
val SPARK_VERSION = "3.5.5"

ThisBuild / Test / parallelExecution := false

assembly / assemblyMergeStrategy := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  // Strip JAR signature files to avoid SecurityException
  case x
      if x.startsWith("META-INF/") &&
        (x.endsWith(".SF") || x.endsWith(".DSA") || x.endsWith(".RSA")) =>
    MergeStrategy.discard
  // Service-provider files must be concatenated so all implementations
  // are registered (e.g. Hadoop's LocalFileSystem + GCS FileSystem).
  case x if x.startsWith("META-INF/services/") => MergeStrategy.concat
  case x                                       => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "org.apache.spark" %% "spark-avro" % SPARK_VERSION,
  "org.apache.spark" %% "spark-hadoop-cloud" % SPARK_VERSION exclude ("stax", "stax-api"),
  // "org.eclipse.rdf4j" % "rdf4j" % "5.1.0",
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

// Required JVM options for Java 9+ module system (Spark compatibility)
// Memory settings for the forked JVM (SBT_OPTS only affects sbt itself, not forked processes)
Compile / run / javaOptions ++= Seq(
  "-Xms2g",
  "-Xmx8g",
  "-XX:+UseG1GC",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
)
