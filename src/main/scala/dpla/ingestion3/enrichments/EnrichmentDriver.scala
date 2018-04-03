package dpla.ingestion3.enrichments

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.model._
import dpla.ingestion3.enrichments.normalizations.StandardNormalizations._

import scala.util.Try


class EnrichmentDriver(conf: i3Conf) extends Serializable {
  /**
    * Reads Twofishes hostname and port from application config file
    *
    * @see dpla.ingestion3.enrichments.Twofisher
    * @see SpatialEnrichmentIntegrationTest
    */
  object Geocoder extends Twofisher {
    override def hostname: String = conf.twofishes.hostname.getOrElse("localhost")
    override def port: String = conf.twofishes.port.getOrElse("8081")
  }

  val dateEnrichment = new ParseDateEnrichment()
  val spatialEnrichment = new SpatialEnrichment(Geocoder)
  val languageMapper = new LanguageMapper
  val typeMapper = new TypeMapper

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
  def enrich(record: OreAggregation): Try[OreAggregation] = Try {

    val enriched = record
      .deDuplicate
      .standardStringEnrichments

    enriched.copy(
      sourceResource = enriched.sourceResource.copy(
        date = enriched.sourceResource.date.map(d => dateEnrichment.parse(d)),
        language = enriched.sourceResource.language.map(languageMapper.enrichLanguage),
        `type` = enriched.sourceResource.`type`.flatMap(typeMapper.enrich)

        // place = enriched.sourceResource.place.map(p => spatialEnrichment.enrich(p))
        )
      )
  }

}
