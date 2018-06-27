package dpla.ingestion3.messages

/**
  * IngestMessage
  *
  * @param message Brief description of the issue
  * @param level ERROR, WARN, INFO
  * @param id Provider ID
  * @param field The source of the problem (not mapping destination!)
  * @param value The original value (if available)
  * @param enrichedValue
  */
case class IngestMessage(message: String,
                         level: String = "WARN",
                         id: String,
                         field: String,
                         value: String = "_missing_"
                         // enrichedValue: Option[String] = Some("No enriched value available")
                        ) {

  def formatMessage(): String = {
    s"${level.toUpperCase} $message $field $id $value"
  }
}
