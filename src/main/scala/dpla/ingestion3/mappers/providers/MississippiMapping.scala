package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{
  AtLeastOne,
  ExactlyOne,
  ZeroToMany,
  ZeroToOne
}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonDSL._
import org.json4s._

class MississippiMapping
    extends JsonMapping
    with JsonExtractor
    with IngestMessageTemplates {

  val formatBlockList: Set[String] = ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("mississippi")

  // TODO confirm this is the most stable identifier for these records
  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString(unwrap(data) \ "@id")

  // OreAggregation
  //  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
  //    extractStrings(unwrap(data) \ "dataProvider").map(nameOnlyAgent)

  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  // TODO confirm no edm rights
  //  override def edmRights(data: Document[json4s.JValue]): ZeroToMany[URI] =
  //    extractStrings(unwrap(data) \ "rights").map(URI)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "delivery" \ "availabilityLinksUrl")
      .map(stringOnlyWebResource)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] = {
    (unwrap(data) \ "delivery" \ "link")
      .filter(node =>
        extractString(node \ "displayLabel")
          .getOrElse("")
          .equalsIgnoreCase("thumbnail")
      )
      .flatMap(node => extractStrings(node \ "linkURL"))
      .map(stringOnlyWebResource)
  }

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "creator")
      .map(nameOnlyAgent)

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "contributor")
      .map(nameOnlyAgent)

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "description")

  // TODO examine format block list values
  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "format")

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "identifier")

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "language")
      .map(nameOnlyConcept)

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "publisher")
      .map(nameOnlyAgent)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "rights")

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "subject")
      .map(nameOnlyConcept)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "creationdate")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "title")

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "type")

  def agent: EdmAgent = EdmAgent(
    name = Some("Mississippi Digital Library"), // TODO Confirm hub name
    uri = Some(URI("http://dp.la/api/contributor/mississippi-digital-library"))
  )
}
