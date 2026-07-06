package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.date.DateBuilder
import dpla.ingestion3.model._
import dpla.ingestion3.enrichments.normalizations.StandardNormalizations._
import dpla.ingestion3.model.DplaMapData.ZeroToOne

import scala.util.Try

class EnrichmentDriver extends Serializable {

  private val dateEnrichment = new DateBuilder()
  val languageEnrichment = new LanguageEnrichment
  val typeEnrichment = new TypeEnrichment
  private val wikiEntityEnrichment = new WikiEntityEnrichment

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
  private def createDataProviderUri(name: ZeroToOne[String]): ZeroToOne[URI] =
    name.map { dataProvider =>
      val providerLabel = dataProvider.toLowerCase // lowercase
        .trim // remove leading and trailing whitespace
        .replaceAll("( )+", "-") // replace 1-n whitespace with single -
        .replaceAll("[^a-z0-9s-]", "") // remove non-alphanumeric chars
      URI(s"$dplaContributorUri$providerLabel")
    }
    // Returns None (instead of throwing) when name is None.
    // Records with no recognized dataProvider code will have no dataProvider URI
    // rather than being silently dropped during enrichment.

  private def enrichDataProvider(record: OreAggregation): EdmAgent = {
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

    val result = enriched.copy(
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

    // Drop records with no resolved dataProvider URI — unrecognized institution
    // codes produce no name, which produces no URI. require() throws inside Try,
    // so EnrichExecutor's flatMap(_.toOption) silently discards these records.
    require(
      result.dataProvider.uri.isDefined,
      s"No dataProvider URI for record: dataProvider.name=${result.dataProvider.name.getOrElse("unknown")}"
    )

    result
  }

}
