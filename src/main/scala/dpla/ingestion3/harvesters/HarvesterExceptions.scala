package dpla.ingestion3.harvesters

/**
  * Harvester Exceptions
  */
object HarvesterExceptions {

  def throwMissingArgException(arg: String) = {
    val msg = s"Missing argument: $arg"
    throw new IllegalArgumentException(msg)
  }

  def throwUnrecognizedArgException(arg: String) = {
    val msg = s"Unrecognized argument: $arg"
    throw new IllegalArgumentException(msg)
  }

  def throwValidationException(arg: String) = {
    val msg = s"Validation error: $arg"
    throw new IllegalArgumentException(msg)
  }
}
