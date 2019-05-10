package dpla.ingestion3.enrichments.normalizations.filters

import dpla.ingestion3.enrichments.normalizations.FilterList
import dpla.ingestion3.enrichments.normalizations.FilterRegex._


/**
  * List of file format, digital surrogate and content-type values to be removed.
  * This filter is most commonly applied to the sourceResource.format field
  */
object DigitalSurrogateBlockList extends FilterList {
  lazy val termList: Set[String] = getTermsFromFiles.map(_.blockListRegex)

  // Defines where to get digital surrogate and format block terms
  override val files: Seq[String] = Seq(
    // TODO This file list should be stored in a config file and not hard coded
    "/formats/dc.csv",
    "/formats/in.csv",
    "/formats/ohio.csv",
    "/formats/p2p.csv",
    "/formats/vt.csv",
    "/formats/iana-imt-types.csv"
  )
}
