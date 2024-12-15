package dpla.ingestion3.harvesters

/** Harvester Exceptions
  */
object HarvesterExceptions {

  def throwMissingArgException(arg: String): Nothing = {
    val msg = s"Missing argument: $arg"
    throw new IllegalArgumentException(msg)
  }

  def throwUnrecognizedArgException(arg: String): Nothing = {
    val msg = s"Unrecognized argument: $arg"
    throw new IllegalArgumentException(msg)
  }

  def throwValidationException(arg: String): Nothing = {
    val msg = s"Validation error: $arg"
    throw new IllegalArgumentException(msg)
  }
}
