package dpla.ingestion3.enrichments

import dpla.ingestion3.model.OreAggregation

/**
  * Standard enrichments.
  *
  */
object StandardEnrichmentUtils {

  implicit class StandardRecordEnrichments(record: OreAggregation) {

    lazy val deDuplicationEnrichment = new DeDuplicationEnrichment
    lazy val stringEnrichments = new StringEnrichments

    def deDuplicate: OreAggregation = deDuplicationEnrichment.enrich(record)

    def standardStringEnrichments: OreAggregation =
      stringEnrichments.enrich(record)
  }
}
