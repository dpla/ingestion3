package dpla.ingestion3.harvesters.api

// Represents a single page response from a CDL harvest.
sealed trait CdlResponse


/**
  *
  * @param queryParams
  * @param url
  * @param text
  */
case class CdlSource(queryParams: Map[String, String],
                     url: Option[String] = None,
                     text: Option[String] = None) extends CdlResponse

/**
  *
  * @param records
  */
case class RecordsPage(records: Seq[CdlRecord]) extends CdlResponse

/**
  *
  * @param message
  * @param errorSource
  */
case class CdlError(message: String,
                    errorSource: CdlSource) extends CdlResponse

/**
  *
  * @param id
  * @param document
  */
case class CdlRecord(id: String,
                     document: String)

// FIXME this is not used, sets are built from item metadata
case class SetsPage(sets: Seq[CdlSet]) extends CdlResponse

case class CdlSet(id: String,
                  document: String,
                  setSource: CdlSource)
