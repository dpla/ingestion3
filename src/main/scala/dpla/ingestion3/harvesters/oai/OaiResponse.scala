package dpla.ingestion3.harvesters.oai

// Represents a single page response from an OAI harvest.
sealed trait OaiResponse

// Url and text are optional b/c there may be errors in formulating the URL or
// parsing the XML response.

case class OaiSource(queryParams: Map[String, String],
                     url: Option[String] = None,
                     text: Option[String] = None) extends OaiResponse

// A single page of successfully parsed records.

case class RecordsPage(records: Seq[OaiRecord]) extends OaiResponse

// A single page of successfully parsed sets.
case class SetsPage(sets: Seq[OaiSet]) extends OaiResponse

// An error that occurs during the harvest, but that does not cause total failure.
case class OaiError(message: String, source: OaiSource) extends OaiResponse

// A single record from an OAI Harvest.
case class OaiRecord(id: String,
                     document: String,
                     setIds: Seq[String],
                     recordSource: OaiSource)

// A single set from an OAI harvest.
case class OaiSet(id: String,
                  document: String,
                  setSource: OaiSource)

// A single page response from an Oai feed.  The page may include an error message.
case class OaiPage(page: String) extends OaiResponse
