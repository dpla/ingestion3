package dpla.ingestion3.enrichments.normalizations

import dpla.ingestion3.model.OreAggregation

/** Standard enrichments.
  */
object StandardNormalizations {

  implicit class StandardRecordNormalizations(record: OreAggregation) {

    lazy val deDuplicationEnrichment = new Deduplication
    lazy val stringEnrichments = new StringNormalizations

    def deDuplicate: OreAggregation = deDuplicationEnrichment.enrich(record)

    def standardStringEnrichments: OreAggregation =
      stringEnrichments.enrich(record)
  }
}
