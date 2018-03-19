package dpla.ingestion3.enrichments.filters

import dpla.ingestion3.enrichments.FilterList
import dpla.ingestion3.enrichments.FilterRegex._


/**
  * List of file format, digital surrogate and content-type values to be removed.
  * This filter is most commonly applied to the sourceResource.format field
  */
object DigitalSurrogateBlockList extends FilterList {
  lazy val termList: Set[String] = getTermsFromFiles.map(_.blockListRegex)

  // Where to get digital surrogate block list terms
  override val files: Seq[String] = Seq(
    // TODO This file list should be stored in a config file and not hard coded
    "/formats/ohio.csv",
    "/formats/iana-imt-types.csv"
  )
}
