package dpla.ingestion3.harvesters.oai.refactor

// Represents a single page response from an OAI harvest.
sealed trait OaiResponse

// A single record from an OAI Harvest.
case class OaiRecord(id: String, document: String, setIds: Seq[String])

// A single set from an OAI harvest.
case class OaiSet(id: String, document: String)

// A single page response from an Oai feed.  The page may include an error message.
case class OaiPage(page: String) extends OaiResponse

trait OaiError

object OaiError {
  def errorForCode(code: String): OaiError = code match {
    case "badArgument" => BadArgument()
    case "badResumptionToken" => BadResumptionToken()
    case "badVerb" => BadVerb()
    case "cannotDisseminateFormat" => CannotDisseminateFormat()
    case "idDoesNotExist" => IdDoesNotExist()
    case "noRecordsMatch" => NoRecordsMatch()
    case "noMetadataFormats" => NoMetadataFormats()
    case "noSetHierarchy" => NoSetHierarchy()
    case _ => throw new RuntimeException("No error for code: " + code)

  }
}

case class BadArgument() extends OaiResponse with OaiError

case class BadResumptionToken() extends OaiResponse with OaiError

case class BadVerb() extends OaiResponse with OaiError

case class CannotDisseminateFormat() extends OaiResponse with OaiError

case class IdDoesNotExist() extends OaiResponse with OaiError

case class NoRecordsMatch() extends OaiResponse with OaiError

case class NoMetadataFormats() extends OaiResponse with OaiError

case class NoSetHierarchy() extends OaiResponse with OaiError
