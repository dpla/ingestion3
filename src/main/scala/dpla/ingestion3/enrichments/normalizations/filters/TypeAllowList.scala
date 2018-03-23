package dpla.ingestion3.enrichments.normalizations.filters

import dpla.ingestion3.enrichments.normalizations.FilterList
import dpla.ingestion3.enrichments.normalizations.FilterRegex._


/**
  * List of type terms that are allowed in the type field because they can be mapped to boarder DCMIType terms
  */
object TypeAllowList extends FilterList {
  lazy val termList: Set[String] = getTermsFromFiles
    .map(line => line.split(",")(0))
    .map(_.allowListRegex)

  override val files: Seq[String] = Seq(
    "/types/dpla-type-normalization.csv"
  )
}
