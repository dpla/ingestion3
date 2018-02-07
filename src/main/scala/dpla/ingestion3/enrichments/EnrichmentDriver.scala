package dpla.ingestion3.enrichments

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.model._
import dpla.ingestion3.enrichments.StandardEnrichmentUtils._

import scala.util.Try


class EnrichmentDriver(conf: i3Conf) extends Serializable {
  /**
    * Reads Twofishes hostname and port from application config file
    *
    * @see dpla.ingestion3.enrichments.Twofisher
    * @see SpatialEnrichmentIntegrationTest
    */
  object Geocoder extends Twofisher {
    override def hostname = conf.twofishes.hostname.getOrElse("localhost")
    override def port = conf.twofishes.port.getOrElse("8081")
  }

  val dateEnrichment = new ParseDateEnrichment()
  val spatialEnrichment = new SpatialEnrichment(Geocoder)
  val langEnrichment = LanguageMapper

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
        language = enriched.sourceResource.language.map(l => LanguageMapper.mapLanguage(l)),
        place = enriched.sourceResource.place.map(p => spatialEnrichment.enrich(p)),
        `type` = enriched.sourceResource.`type`.map(t => {
          // Cast original type value to lower case and map it to DCMIType IRIs (@see DcmiTypeMap).
          // If it can be mapped to a valid DCMIType IRI then lookup the local label for that IRI
          // and then convert label to lower case.
            DcmiTypeMapper.mapDcmiType(t.toLowerCase) match {
              case Some(typeIri) => DcmiTypeStringMapper.mapDcmiTypeString(typeIri).toLowerCase
            }
          }
        )
      ))
  }

}
