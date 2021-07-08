package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JValue
import org.json4s.JsonDSL._

class CdlMapping extends JsonMapping with JsonExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: String = "cdl"

  // OreAggregation fields
  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] = extractString("id")(data)

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] = Seq(nameOnlyAgent(getDataProvider(data)))

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    extractStrings("rights_uri")(data)
    .map(URI)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] = Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] = thumbnail(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("California Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/cdl"))
  )

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] = providerUri(data)

  // SourceResource
  override def alternateTitle(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("alternative_title_ss")(data)

  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    extractStrings("collection_name")(data).map(nameOnlyCollection)

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings("contributor_ss")(data).map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings("creator_ss")(data).map(nameOnlyAgent)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings("date_ss")(data).map(stringOnlyTimeSpan)

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("description")(data)

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("extent_ss")(data) ++
      extentFromFormat(data)

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("format")(data)
      .map(_.applyBlockFilter(
        DigitalSurrogateBlockList.termList ++
        FormatTypeValuesBlockList.termList ++
        ExtentIdentificationList.termList))
      .filter(_.nonEmpty)

  override def genre(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings("genre_ss")(data).map(nameOnlyConcept)

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("identifier_ss")(data)

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings("language_ss")(data).map(nameOnlyConcept)

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    extractStrings("coverage_ss")(data).map(nameOnlyPlace)

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings("publisher_ss")(data).map(nameOnlyAgent)

  override def relation(data: Document[JValue]): ZeroToMany[LiteralOrUri] =
    extractStrings("relation_ss")(data).map(eitherStringOrUri)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractStrings("rights_ss")(data) ++
      extractStrings("rights_note_ss")(data) ++
      extractStrings("rights_date_ss")(data) ++
      extractStrings("rightsholder_ss")(data)

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings("subject_ss")(data)
      .map(_.cleanupLeadingPunctuation)
      .map(_.cleanupEndingPunctuation)
      .map(_.stripEndingPeriod)
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def temporal(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings("temporal_ss")(data).map(stringOnlyTimeSpan)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings("title_ss")(data).map(_.stripBrackets)

  override def `type`(data: Document[JValue]): ZeroToMany[String] = extractStrings("type")(data)

  override def intermediateProvider(data: Document[json4s.JValue]): ZeroToOne[EdmAgent] = {
    val repositories = extractStrings("repository_name")(data)
    if(repositories.tail.nonEmpty)
      repositories.take(2).lastOption.map(nameOnlyAgent)
    else
      None
  }

  // Helper methods
  def getDataProvider(json: JValue): String = {
    val campus = extractStrings("campus_name")(json).headOption
    val repository = extractStrings("repository_name")(json).headOption

    (campus, repository) match {
      case (Some(campusVal), Some(repositoryVal)) => campusVal + ", " + repositoryVal
      case (None, Some(repositoryVal)) => repositoryVal
      case _ => throw new Exception("Unable to determine provider.")
    }
  }

  def thumbnail(json: JValue): ZeroToMany[EdmWebResource] =
    extractStrings("reference_image_md5")(json)
      .map(md5 => stringOnlyWebResource("https://thumbnails.calisphere.org/clip/150x150/" + md5))

  def providerUri(json: JValue): ZeroToMany[EdmWebResource] = extractStrings("url_item")(json).map(stringOnlyWebResource)

  /**
    * Extracts values from format field and returns only those values which appear to be
    * extent statements
    *
    * @param data Original record
    * @return ZeroToMany[String] Extent values
    */
  def extentFromFormat(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("format")(data)
      .map(_.extractExtents)
      .filter(_.nonEmpty)
}
