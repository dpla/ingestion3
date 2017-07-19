package dpla.ingestion3.enrichments

import dpla.ingestion3.model._


class EnrichmentDriver {
  val stringEnrich = new StringEnrichments()
  val dateEnrich = new ParseDateEnrichment()

  /**
    * Applies a set of common enrichments that need to be run for all providers
    *   * Spatial
    *   * Language
    *   * Type
    *   * Date
    *
    * @param record The mapped record
    * @return An enriched record
    */
  def enrich(record: DplaMapData): DplaMapData = {
    record.copy(
      DplaSourceResource(
        date = record.sourceResource.date.map(d => dateEnrich.parse(d)),
        language = record.sourceResource.language.map(LanguageMapper.mapLanguage)
    ))
  }
}
