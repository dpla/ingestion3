package dpla.ingestion3.messages

trait IngestMessageTemplates {
  def mintUriError(id: String, field: String, value: String, msg: Option[String] = None): IngestMessage =
    IngestMessage(
      message = s"Unable to mint URI ${msg.getOrElse("NO URI")}".trim,
      level = IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def missingRequiredError(id: String, field: String): IngestMessage =
    IngestMessage(
      message = s"Missing required field",
      level = IngestLogLevel.error,
      id = id,
      field = field,
      value = "MISSING"
    )

  def enrichedValue(id: String, field: String, origValue: String, enrichValue: String): IngestMessage =
    IngestMessage(
      message = s"Enriched value",
      level = IngestLogLevel.info,
      id = id,
      field = field,
      value = origValue,
      enrichedValue = "Not enriched"
    )

  def originalValue(id: String, field: String, value: String): IngestMessage =
    IngestMessage(
      message = s"Original value",
      level = IngestLogLevel.info,
      id = id,
      field = field,
      value = value,
      enrichedValue = "Not enriched"
    )

  def emptyRecord(): IngestMessage =
    IngestMessage(
      message = s"Empty record",
      level = IngestLogLevel.error,
      id = "Unknown",
      field = "N/A",
      value = "N/A"
    )
}
