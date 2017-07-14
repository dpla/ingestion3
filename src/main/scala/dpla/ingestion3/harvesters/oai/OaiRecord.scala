package dpla.ingestion3.harvesters.oai

// Represents a single record from an OAI Harvest.
case class OaiRecord(id: String, document: String, set: Option[OaiSet] = None)
