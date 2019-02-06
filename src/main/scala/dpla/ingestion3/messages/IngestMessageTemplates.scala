package dpla.ingestion3.messages

trait IngestMessageTemplates {

  def moreThanOneValueInfo(id: String, field: String, value: String, msg: Option[String] = None): IngestMessage =
    IngestMessage(
      message = s"More than one value provided to single value field".trim,
      level = IngestLogLevel.info,
      id = id,
      field = field,
      value = value
    )

  def mintUriError(id: String, field: String, value: String, msg: Option[String] = None): IngestMessage =
    IngestMessage(
      message = s"Unable to mint URI ${msg.getOrElse("No URI")}".trim,
      level = IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def missingRecommendedWarning(id: String, field: String): IngestMessage =
    IngestMessage(
      message = s"Missing recommended field",
      level = IngestLogLevel.warn,
      id = id,
      field = field,
      value = "MISSING"
    )

  def missingRequiredError(id: String, field: String): IngestMessage =
    IngestMessage(
      message = s"Missing required field",
      level = IngestLogLevel.error,
      id = id,
      field = field,
      value = "MISSING"
    )

  def missingRights(id: String): IngestMessage =
    IngestMessage(
      message = s"Missing required field",
      level = IngestLogLevel.error,
      id = id,
      field = "rights or edmRights",
      value = ""
    )

  def duplicateRights(id: String): IngestMessage =
    IngestMessage(
      message = s"Duplicate",
      level = IngestLogLevel.warn,
      id = id,
      field = "rights and edmRights",
      value = "both rights and edmRights are defined"
    )

  def duplicateOriginalId(id: String): IngestMessage =
    IngestMessage(
      message = "Duplicate",
      level = IngestLogLevel.error,
      id = id,
      field = "originalId",
      value = "at least one other record shares this originalId"
    )

  def enrichedValue(id: String, field: String, origValue: String, enrichValue: String): IngestMessage =
    IngestMessage(
      message = s"Enriched value",
      level = IngestLogLevel.info,
      id = id,
      field = field,
      value = origValue,
      enrichedValue = enrichValue
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

  def exception(id: String, exception: Throwable): IngestMessage =
    IngestMessage(
      message = s"Exception (see error report)",
      level = IngestLogLevel.error,
      id = id,
      field = "",
      value = exception.getMessage.replaceAll("\n", " | ")
    )
}
