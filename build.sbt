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
  // Properly merge binary Log4j2Plugins.dat plugin caches from all JARs using
  // PluginCache, so every converter registration (%d, %t, %level, %logger…)
  // is preserved. filterDistinctLines and first both corrupt or discard the
  // binary format; this is the correct approach per sbt/sbt-assembly#501.
  // log4j-core must be on the build classpath (see project/plugins.sbt).
  case "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat" =>
    new sbtassembly.MergeStrategy {
      val name = "log4j2PluginCache"
      def apply(tempDir: java.io.File, path: String, files: Seq[java.io.File]): Either[String, Seq[(java.io.File, String)]] = {
        import org.apache.logging.log4j.core.config.plugins.processor.PluginCache
        import java.util.Collections
        val merged = new PluginCache
        files.foreach { f =>
          merged.loadCacheFiles(
            Collections.enumeration(java.util.Arrays.asList(f.toURI.toURL))
          )
        }
        val out = java.io.File.createTempFile("Log4j2Plugins", ".dat", tempDir)
        val os  = new java.io.FileOutputStream(out)
        try { merged.writeCache(os) } finally { os.close() }
        Right(Seq(out -> path))
      }
    }
  case x => MergeStrategy.first
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
// Memory settings for the forked JVM (SBT_OPTS only affects sbt itself, not forked processes;
// to change IngestRemap/NaraMergeUtil heap, change -Xmx here, not in the shell script)
Compile / run / javaOptions ++= Seq(
  "-Xms4g",
  "-Xmx14g",
  "-XX:+UseG1GC",
  "-XX:MaxGCPauseMillis=200",
  "-Dspark.sql.parquet.enableVectorizedReader=false",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
)
