resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.2.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Required so build.sbt can use PluginCache to properly merge Log4j2Plugins.dat
// binary files during assembly (instead of corrupting them with filterDistinctLines).
// Version matches what Spark 3.5.x pulls in transitively.
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.20.0"