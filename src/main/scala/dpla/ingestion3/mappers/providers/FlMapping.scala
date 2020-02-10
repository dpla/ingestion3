package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.enrichments.TaggingUtils._
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JsonDSL._
import org.json4s._


class FlMapping extends JsonMapping with JsonExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] = ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "florida"

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractStrings(unwrap(data) \ "isShownAt").headOption

  // OreAggregation
  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "dataProvider").map(nameOnlyAgent)

  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def edmRights(data: Document[json4s.JValue]): ZeroToMany[URI] =
    extractStrings(unwrap(data) \ "sourceResource" \ "rights" \ "@id")
      .filter(_.nonEmpty) // FIXME filtering non-empty values should be a standard edmRights normalization
      .map(URI)

  override def intermediateProvider(data: Document[JValue]): ZeroToOne[EdmAgent] =
    extractString(unwrap(data) \ "intermediateProvider")
      .map(nameOnlyAgent)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "isShownAt")
      .map(stringOnlyWebResource)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "preview").map(stringOnlyWebResource)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def tags(data: Document[JValue]): ZeroToMany[URI] =
    description(data).flatMap(_.applyAviationTags)

  // SourceResource
  override def alternateTitle(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "alternative")

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data)  \ "sourceResource" \ "contributor" \ "name")
      .map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data)  \ "sourceResource" \ "creator" \ "name")
      .map(nameOnlyAgent)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractDate(unwrap(data) \ "sourceResource" \ "date")

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "description")

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "extent")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "format") ++
      extractStrings(unwrap(data) \ "sourceResource" \ "genre" \ "name")

  override def genre(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data) \ "sourceResource" \ "genre" \ "name")
      .map(nameOnlyConcept)

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "identifier")

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    (extractStrings(unwrap(data) \ "sourceResource" \ "language" \ "name") ++
      extractStrings(unwrap(data) \ "sourceResource" \ "language" \ "iso_639_3"))
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    extractStrings(unwrap(data) \ "sourceResource" \ "spatial" \ "name")
      .map(nameOnlyPlace)

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "sourceResource" \ "publisher")
      .map(nameOnlyAgent)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "rights" \ "text")

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data)  \ "sourceResource" \ "subject" \ "name")
      .map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "title")

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "sourceResource" \ "type")

  def agent = EdmAgent(
    name = Some("Sunshine State Digital Network"),
    uri = Some(URI("http://dp.la/api/contributor/florida"))
  )

  def extractDate(date: JValue): ZeroToMany[EdmTimeSpan] = {
    iterify(date).children.map(d =>
      EdmTimeSpan(
        begin = extractString(d \ "begin"),
        end = extractString(d \ "end"),
        originalSourceDate = extractString(d \ "displayDate")
      ))
  }
}
