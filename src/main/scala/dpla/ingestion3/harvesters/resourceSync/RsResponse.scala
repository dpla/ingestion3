package dpla.ingestion3.harvesters.resourceSync

sealed trait RsResponse

case class RsSource(url: Option[String] = None,
                    text: Option[String] = None) extends RsResponse

// A list of items
case class RsResourceList(records: Seq[RsRecord]) extends RsResponse

// A list of ResourceLists
case class RsResourceIndex(records: Seq[RsResourceList]) extends RsResponse


// An error that occurs during the harvest, but that does not cause total failure.
case class RsError(message: String,
                    errorSource: RsSource) extends RsResponse

// A single record from an Resource Sync harvest.
case class RsRecord(id: String,
                    document: String,
                    recordSource: RsSource) extends RsResponse

// TODO this is a bit funky
case class RsResourceCapability(

                               )


case class RsHarvesterException(message: String) extends Exception(message)