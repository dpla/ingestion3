package dpla.ingestion3.enrichments

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.enrichments.date.DateBuilder
import dpla.ingestion3.model._
import dpla.ingestion3.enrichments.normalizations.StandardNormalizations._
import dpla.ingestion3.model.DplaMapData.ZeroToOne

import scala.util.Try

class EnrichmentDriver(conf: i3Conf) extends Serializable {

  val dateEnrichment = new DateBuilder()
  // val spatialEnrichment = new SpatialEnrichment(Geocoder)
  val languageEnrichment = new LanguageEnrichment
  val typeEnrichment = new TypeEnrichment
  val wikiEntityEnrichment = new WikiEntityEnrichment

  lazy private val dplaContributorUri = "http://dp.la/api/contributor/"

  /** Create a DPLA URI for each dataProvider
    *
    * Lowercase label, strip excessive whitespace, remove non-alphanumeric
    * characters
    *
    * @param name
    *   Label of dataProvider
    * @return
    */
  def createDataProviderUri(name: ZeroToOne[String]): ZeroToOne[URI] =
    name match {
      case Some(dataProvider) =>
        val providerLabel = dataProvider.toLowerCase // lowercase
          .trim // remove leading and trailing whitespace
          .replaceAll("( )+", "-") // replace 1-n whitespace with single -
          .replaceAll("[^a-z0-9s-]", "") // remove non-alphanumeric chars
        Some(URI(s"$dplaContributorUri$providerLabel"))
      case None =>
        throw new RuntimeException(
          "Missing required field Data Provider name when minting URI"
        )
    }

  def enrichDataProvider(record: OreAggregation): EdmAgent = {
    val enrichedWithWikiEntity: EdmAgent = wikiEntityEnrichment.enrichEntity(
      record.dataProvider,
      Option(record.provider)
    )
    val uri = createDataProviderUri(record.dataProvider.name)
    enrichedWithWikiEntity.copy(uri = uri)
  }

  /** Applies a set of common enrichments that need to be run for all providers
    * * Spatial * Language * Type * Date
    *
    * @param record
    *   The mapped record
    * @return
    *   An enriched record
    */
  def enrich(record: OreAggregation): Try[OreAggregation] = Try {

    val enriched = record.deDuplicate.standardStringEnrichments

    enriched.copy(
      provider = wikiEntityEnrichment.enrichEntity(enriched.provider),
      dataProvider = enrichDataProvider(enriched),
      sourceResource = enriched.sourceResource.copy(
        date = enriched.sourceResource.date
          .map(date => dateEnrichment.generateBeginEnd(date.originalSourceDate))
          .distinct,
        language = enriched.sourceResource.language
          .map(languageEnrichment.enrichLanguage)
          .distinct,
        `type` =
          enriched.sourceResource.`type`.flatMap(typeEnrichment.enrich).distinct
      )
    )
  }

}
