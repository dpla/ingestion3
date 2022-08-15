package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.TaggingUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JsonDSL._
import org.json4s._


class CommunityWebsMapping extends JsonMapping with JsonExtractor with IngestMessageTemplates {
  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("community-webs")

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractStrings(unwrap(data) \ "id").headOption

  // OreAggregation
  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "data_provider").map(nameOnlyAgent)

  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def edmRights(data: Document[json4s.JValue]): ZeroToMany[URI] =
    extractStrings(unwrap(data) \ "rights_statement")
      .filter(_.nonEmpty) // FIXME filtering non-empty values should be a standard edmRights normalization
      .map(URI)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "preview")
      .map(stringOnlyWebResource)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "preview_image").map(stringOnlyWebResource)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
//  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
//    extractStrings(unwrap(data) \ "sourceResource" \ "contributor" \ "name")
//      .map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "creator")
      .map(nameOnlyAgent)

  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    extractStrings(unwrap(data) \ "collection")
      .map(nameOnlyCollection)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings(unwrap(data) \ "date")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "description")

//  override def extent(data: Document[JValue]): ZeroToMany[String] =
//    extractStrings(unwrap(data) \ "sourceResource" \ "extent")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "type")

//  override def genre(data: Document[JValue]): ZeroToMany[SkosConcept] =
//    extractStrings(unwrap(data) \ "sourceResource" \ "genre" \ "name")
//      .map(nameOnlyConcept)

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "identifier")

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data) \ "language")
      .flatMap(_.split(";"))
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    extractStrings(unwrap(data) \ "coverage")
      .map(nameOnlyPlace)

//  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
//    extractStrings(unwrap(data) \ "sourceResource" \ "publisher")
//      .map(nameOnlyAgent)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \ "rights_free_text")

  override def relation(data: Document[JValue]): ZeroToMany[LiteralOrUri] =
    extractStrings(unwrap(data) \ "relation")
      .map(relation => LiteralOrUri(relation, isUri = false))

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data) \ "subject")
      .flatMap(_.split(";"))
      .map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \ "title")

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "type")

  def agent = EdmAgent(
    name = Some("Community Webs"),
    uri = Some(URI("http://dp.la/api/contributor/community-webs"))
  )
}
