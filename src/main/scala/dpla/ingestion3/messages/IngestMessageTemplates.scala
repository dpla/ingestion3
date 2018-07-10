package dpla.ingestion3.messages

import java.net.URI

import scala.util.{Failure, Success, Try}

trait IngestMessageTemplates {
  def mintUriError(id: String, field: String, value: String, msg: Option[String] = None): IngestMessage =
    IngestMessage(
      message = s"Unable to mint URI ${msg.getOrElse("NO URI")}".trim,
      level = IngestLogLevel.error,
      id = id,
      field = field,
      value = value
    )

  def missingRequiredError(id: String, field: String): IngestMessage =
    IngestMessage(
      message = s"Missing required field",
      level = "ERROR",
      id = id,
      field = field,
      value = "MISSING"
    )

  def enrichedValue(id: String, field: String, origValue: String, enrichValue: String): IngestMessage =
    IngestMessage(
      message = s"Enriched value",
      level = "INFO",
      id = id,
      field = field,
      value = origValue
      // enrichedValue = Option(enrichValue) // TODO fixup and uncomment
    )

  def originalValue(id: String, field: String, value: String): IngestMessage =
    IngestMessage(
      message = s"Original value",
      level = "INFO",
      id = id,
      field = field,
      value = value
      // enrichedValue = Option("Not enriched") // TODO see above
    )
}

trait IngestValidations extends IngestMessageTemplates {
  def validateUri(uriStr: String): Try[URI] = Try { new URI(uriStr) }
}