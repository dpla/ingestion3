package dpla.ingestion3.messages

import java.net.URI

import scala.util.{Failure, Success, Try}

trait IngestErrors {
  def mintUriError(id: String, msg: Option[String] = None, field: String, value: String): IngestMessage =
    IngestMessage(
      message = s"Unable to mint URI: $msg",
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
}

trait IngestValidations extends IngestErrors{
  def validateUri(uriStr: String): Try[URI] = Try { new URI(uriStr) }
}