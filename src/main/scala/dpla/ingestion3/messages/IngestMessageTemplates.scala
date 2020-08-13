package dpla.ingestion3.messages

trait IngestMessageTemplates {

  def moreThanOneValueMsg(id: String, field: String, value: String,
                          msg: Option[String] = None, enforce: Boolean): IngestMessage =
    IngestMessage(
      message = s"More than one value mapped, one expected".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def mintUriMsg(id: String, field: String, value: String,
                 msg: Option[String] = None, enforce: Boolean): IngestMessage =
    IngestMessage(
      message = s"Unable to mint URI ${msg.getOrElse("No URI")}".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def invalidEdmRightsMsg(id: String, field: String, value: String,
                          msg: Option[String] = None, enforce: Boolean): IngestMessage =
    IngestMessage(
      message = s"Invalid value".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def normalizedEdmRightsMsg(id: String, field: String, value: String,
                          msg: Option[String] = None, enforce: Boolean): IngestMessage =
    IngestMessage(
      message = s"Normalized value".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def missingRecommendedFieldMsg(id: String, field: String): IngestMessage =
    IngestMessage(
      message = s"Missing recommended field",
      level = IngestLogLevel.warn,
      id = id,
      field = field,
      value = "MISSING"
    )

  def missingRequiredFieldMsg(id: String, field: String, enforce: Boolean): IngestMessage =
    IngestMessage(
      message = s"Missing required field",
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = "MISSING"
    )

  def missingRights(id: String, enforce: Boolean): IngestMessage =
    missingRequiredFieldMsg(
      id,
      "rights or edmRights",
      enforce
    )

  def duplicateRights(id: String): IngestMessage =
    IngestMessage(
      message = s"Duplicate",
      level = IngestLogLevel.warn,
      id = id,
      field = "rights and edmRights",
      value = "both rights and edmRights are defined"
    )

  def duplicateOriginalId(id: String, enforce: Boolean): IngestMessage =
    IngestMessage(
      message = "Duplicate",
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
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
      message = s"Exception (see value column)",
      level = IngestLogLevel.error,
      id = id,
      field = "",
      value = exception.getMessage.replaceAll("\n", " | ")
    )
}
