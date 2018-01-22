package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils.{IdMinter, JsonExtractor, Mapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class CdlExtractor() extends Mapping[JValue] with IdMinter[JValue] with JsonExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: String = "cdl"

  override def getProviderId(implicit data: JValue): String = extractString("id")(data)
    .getOrElse(throw new RuntimeException(s"No ID for record: ${compact(data)}"))


  // OreAggregation fields
  override def dplaUri(data: JValue): ExactlyOne[URI] = mintDplaItemUri(data)

  override def sidecar(data: JValue): JValue = ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dataProvider(data: JValue): ExactlyOne[EdmAgent] = nameOnlyAgent(getDataProvider(data))

  override def originalRecord(data: JValue): ExactlyOne[String] = Utils.formatJson(data)

  override def preview(data: JValue): ZeroToOne[EdmWebResource] = thumbnail(data)

  override def provider(data: JValue): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("California Digital Library"),
    uri = Some(new URI("http://dp.la/api/contributor/cdl"))
  )

  override def isShownAt(data: JValue): ExactlyOne[EdmWebResource] = uriOnlyWebResource(providerUri(data))


  // SourceResource
  override def alternateTitle(data: JValue): ZeroToMany[String] = extractStrings("alternative_title_ss")(data)

  override def collection(data: JValue): ZeroToMany[DcmiTypeCollection] =
    extractStrings("collection_name")(data).map(nameOnlyCollection)

  override def contributor(data: JValue): ZeroToMany[EdmAgent] = extractStrings("contributor_ss")(data).map(nameOnlyAgent)

  override def creator(data: JValue): ZeroToMany[EdmAgent] = extractStrings("creator_ss")(data).map(nameOnlyAgent)

  override def date(data: JValue): ZeroToMany[EdmTimeSpan] = extractStrings("date_ss")(data).map(stringOnlyTimeSpan)

  override def description(data: JValue): ZeroToMany[String] = extractStrings("description_ss")(data)

  override def extent(data: JValue): ZeroToMany[String] = extractStrings("extent_ss")(data)

  override def format(data: JValue): ZeroToMany[String] = extractStrings("format")(data)

  override def genre(data: JValue): ZeroToMany[SkosConcept] = extractStrings("genre_ss")(data).map(nameOnlyConcept)

  override def identifier(data: JValue): ZeroToMany[String] = extractStrings("identifier_ss")(data)

  override def language(data: JValue): ZeroToMany[SkosConcept] = extractStrings("language_ss")(data).map(nameOnlyConcept)

  override def place(data: JValue): ZeroToMany[DplaPlace] = extractStrings("coverage_ss")(data).map(nameOnlyPlace)

  override def publisher(data: JValue): ZeroToMany[EdmAgent] = extractStrings("publisher_ss")(data).map(nameOnlyAgent)

  override def relation(data: JValue): ZeroToMany[LiteralOrUri] = extractStrings("relation_ss")(data).map(eitherStringOrUri)

  override def rights(data: JValue): AtLeastOne[String] =
    extractStrings("rights_ss")(data) ++ extractStrings("rights_note_ss")(data) ++ extractStrings("rights_date_ss")(data)

  override def rightsHolder(data: JValue): ZeroToMany[EdmAgent] = extractStrings("rightsholder_ss")(data).map(nameOnlyAgent)

  override def subject(data: JValue): ZeroToMany[SkosConcept] = extractStrings("subject_ss")(data).map(nameOnlyConcept)

  override def temporal(data: JValue): ZeroToMany[EdmTimeSpan] = extractStrings("temporal_ss")(data).map(stringOnlyTimeSpan)

  override def title(data: JValue): AtLeastOne[String] = extractStrings("title_ss")(data)

  override def `type`(data: JValue): ZeroToMany[String] = extractStrings("type")(data)


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

  def thumbnail(json: JValue): Option[EdmWebResource] =
    extractString("reference_image_md5")(json) match {
      case Some(md5) => Some(
        uriOnlyWebResource(
          new URI("https://thumbnails.calisphere.org/clip/150x150/" + md5)
        )
      )
      case None => None
    }

  def providerUri(json: JValue): URI =
    extractString("url_item")(json) match {
      case Some(url) => new URI(url)
      case None => throw new Exception("Unable to determine URL of item on provider's site")
    }
}
