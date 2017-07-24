package dpla.ingestion3.enrichments

import dpla.ingestion3.model._

/**
  * TODO: assign the hostname from a config file or commandline switch?
  *
  * @see dpla.ingestion3.enrichments.Twofisher
  * @see SpatialEnrichmentIntegrationTest
  */
object Geocoder extends Twofisher {
  override def hostname = {
    System.getenv("GEOCODER_HOST") match {
      case h if h.isInstanceOf[String] => h
      case _ => "localhost"
    }
  }
}

class EnrichmentDriver {
  val stringEnrichment = new StringEnrichments()
  val dateEnrichment = new ParseDateEnrichment()
  val spatialEnrichment = new SpatialEnrichment(Geocoder)

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
        date = record.sourceResource.date.map(d => dateEnrichment.parse(d)),
        language = record.sourceResource.language.map(LanguageMapper.mapLanguage),
        place = record.sourceResource.place
                      .map(p => spatialEnrichment.enrich(p))
    ))
  }
}
