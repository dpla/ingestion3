package dpla.ingestion3.entries

object Entry {
  // Module opens are now handled via JVM args in build.sbt
  // This method is kept for backwards compatibility but is a no-op
  def suppressUnsafeWarnings(): Unit = ()
}
