package dpla.ingestion3.messages

trait IngestMessageTemplates {

  def moreThanOneValueMsg(
      id: String,
      field: String,
      value: String,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"More than one value mapped, one expected".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def mintUriMsg(
      id: String,
      field: String,
      value: String,
      msg: Option[String] = None,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"Unable to mint URI ${msg.getOrElse("No URI")}".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def invalidEdmRightsMsg(
      id: String,
      field: String,
      value: String,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"Not a valid edmRights URI".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def multipleEdmRightsMsg(
      id: String,
      field: String,
      value: String,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"Multiple valid edmRights URIs".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def normalizedEdmRightsHttpsMsg(
      id: String,
      field: String,
      value: String,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"Normalized https://".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def normalizedEdmRightsWWWMsg(
      id: String,
      field: String,
      value: String,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"Normalized www".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def normalizedEdmRightsRsPageMsg(
      id: String,
      field: String,
      value: String,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"Normalized /page/ to /vocab/".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def normalizedEdmRightsTrailingSlashMsg(
      id: String,
      field: String,
      value: String,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"Normalized add trailing `/`".trim,
      level = if (enforce) IngestLogLevel.error else IngestLogLevel.warn,
      id = id,
      field = field,
      value = value
    )

  def normalizedEdmRightsTrailingPunctuationMsg(
      id: String,
      field: String,
      value: String,
      enforce: Boolean
  ): IngestMessage =
    IngestMessage(
      message = s"Normalized remove trailing punctuation".trim,
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

  def missingRequiredFieldMsg(
      id: String,
      field: String,
      enforce: Boolean
  ): IngestMessage =
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

  def enrichedValue(
      id: String,
      field: String,
      origValue: String,
      enrichValue: String
  ): IngestMessage =
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

  def exception(id: String, exception: Throwable): IngestMessage =
    IngestMessage(
      message = s"Exception (see value column)",
      level = IngestLogLevel.error,
      id = id,
      field = "",
      value = exception.getMessage.replaceAll("\n", " | ")
    )
}
