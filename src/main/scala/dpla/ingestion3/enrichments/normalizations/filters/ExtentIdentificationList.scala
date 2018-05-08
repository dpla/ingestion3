package dpla.ingestion3.enrichments.normalizations.filters

import dpla.ingestion3.enrichments.normalizations.FilterList
/**
  * Identifies values that are likely extend statements
  * This filter is most commonly applied to the sourceResource.format field
  */
object ExtentIdentificationList extends FilterList {
  lazy val termList: Set[String] = Set(
    "^.*[^a-zA-Z]x[^a-zA-Z].*$", // 1 x 2 OR 1 X 2 OR 1x2 but not 1 xerox
    "^[0-9].*" // starts with number
  )
}

/**
  * Exceptions to the rules outlines above
  */
object ExtentExceptionsList extends FilterList {
  override lazy val termList: Set[String] = Set(
    "mm", // 8mm 16mm
    "millimeter"
  )
}
