package dpla.ingestion3.harvesters.oai.refactor

// Represents a single page response from an OAI harvest.
sealed trait OaiResponse

// An error that occurs during the harvest, but that does not cause total failure.
case class OaiError(message: String, url: Option[String] = None)
    extends OaiResponse

// A single record from an OAI Harvest.
case class OaiRecord(id: String, document: String, setIds: Seq[String])

// A single set from an OAI harvest.
case class OaiSet(id: String, document: String)

// A single page response from an Oai feed.  The page may include an error message.
case class OaiPage(page: String) extends OaiResponse
