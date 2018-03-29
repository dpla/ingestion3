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
        language = enriched.sourceResource.language.map(l => LanguageMapper.enrich(l)),
        place = enriched.sourceResource.place.map(p => spatialEnrichment.enrich(p)),

        /**
          Type enrichment
          ----------------
          Lower case the original string value and try to map to DCMIType IRIs (@see mapDcmiType()).
          If the original type value can be mapped to a valid IRI then mapDcmiTypeToString() maps the
          IRI to a string label and lowercases that label. The lowercase IRI label (without the corresponding
          IRI) is the enriched value.

          Original values that cannot be mapped to a DCMIType IRI are dropped.
         */
        `type` = enriched.sourceResource.`type`
          .flatMap(t => DcmiTypeMapper.mapDcmiType(t.toLowerCase()))
          .map(DcmiTypeStringMapper.mapDcmiTypeString)
          .map(_.toLowerCase)
        )
      )
  }

}
