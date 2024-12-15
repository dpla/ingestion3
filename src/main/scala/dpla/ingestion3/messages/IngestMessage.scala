package dpla.ingestion3.messages

object IngestLogLevel {
  val info: String = "info"
  val warn: String = "warn"
  val error: String = "error"
}

/** IngestMessage
  *
  * @param message
  *   Brief description of the issue
  * @param level
  *   ERROR, WARN, INFO
  * @param id
  *   Provider ID
  * @param field
  *   The source of the problem (not mapping destination!)
  * @param value
  *   The original value (if available)
  * @param enrichedValue
  *   The enriched value, default is empty string
  */
case class IngestMessage(
    message: String,
    level: String,
    id: String,
    field: String,
    value: String = "_missing_",
    enrichedValue: String = ""
)