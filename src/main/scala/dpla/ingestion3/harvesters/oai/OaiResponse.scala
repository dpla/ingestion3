package dpla.ingestion3.harvesters.oai

/** The information that was used to create a request */
case class OaiRequestInfo(
  verb: String,
  metadataPrefix: Option[String],
  set: Option[String],
  resumptionToken: Option[String],
  timestamp: Long
)

/** A single record from an OAI Harvest. */
case class OaiRecord(id: String, document: String, info: OaiRequestInfo)

/** A single set from an OAI harvest. */
case class OaiSet(id: String, document: String, info: OaiRequestInfo)

/** Represents a single response from an OAI harvest. */
sealed trait OaiResponse

/** A single page response from an Oai feed.  The page may include an error message. */
case class OaiPage(page: String, info: OaiRequestInfo) extends OaiResponse

trait OaiError extends OaiResponse

object OaiError {
  def errorForCode(code: String, info: OaiRequestInfo): OaiError = code match {
    case "badArgument" => BadArgument(info)
    case "badResumptionToken" => BadResumptionToken(info)
    case "badVerb" => BadVerb(info)
    case "cannotDisseminateFormat" => CannotDisseminateFormat(info)
    case "idDoesNotExist" => IdDoesNotExist(info)
    case "noRecordsMatch" => NoRecordsMatch(info)
    case "noMetadataFormats" => NoMetadataFormats(info)
    case "noSetHierarchy" => NoSetHierarchy(info)
    case _ => throw new RuntimeException("No error for code: " + code)
  }
}

case class BadArgument(info: OaiRequestInfo) extends OaiError

case class BadResumptionToken(info: OaiRequestInfo) extends OaiError

case class BadVerb(info: OaiRequestInfo) extends OaiError

case class CannotDisseminateFormat(info: OaiRequestInfo) extends OaiError

case class IdDoesNotExist(info: OaiRequestInfo) extends OaiError

case class NoRecordsMatch(info: OaiRequestInfo) extends OaiError

case class NoMetadataFormats(info: OaiRequestInfo) extends OaiError

case class NoSetHierarchy(info: OaiRequestInfo) extends OaiError
