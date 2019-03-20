package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class IllinoisMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      ExtentIdentificationList.termList

  val extentAllowAlist = ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "il"

  // FIXME Should use OAI header \ identifier for ID not dc:identifer  
  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractStrings(data \\ "identifier").find(s => HttpUtils.validateUrl(s))

  // SourceResource mapping
  // FIXME
  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    extractStrings(data \\ "isPartOf")
      .map(nameOnlyCollection)

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    // alternative
    extractStrings(data \\ "alternative")

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // contributor
    extractStrings(data \\ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // contributor
    extractStrings(data \\ "creator")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // dc:date. If not present use <dcterms:created>
    (if (extractStrings(data \\ "date").nonEmpty) {
      extractStrings(data \\ "date")
    } else {
      extractStrings(data \\ "created")
    }).map(stringOnlyTimeSpan)


  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
  // description
    extractStrings(data \\ "description")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
  // format ++ medium
    (extractStrings(data \\ "format") ++ extractStrings(data \\ "medium"))
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
  // language
    extractStrings(data \\ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
  // spatial
    extractStrings(data \\ "spatial")
      .map(nameOnlyPlace)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
  // rights
    extractStrings(data \\ "rights")

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
  // subject
    extractStrings(data \\ "subject")
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // temporal
    extractStrings(data \\ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): ZeroToMany[String] =
  // title
    extractStrings(data \\ "title")

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
  // type
    extractStrings(data \\ "type")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // provenance
  extractStrings(data \\ "provenance")
    .map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // isShownAt
    extractStrings(data \\ "isShownAt")
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // preview
    extractStrings(data \\ "preview")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Illinois Digital Heritage Hub"),
    uri = Some(URI("http://dp.la/api/contributor/il"))
  )
}
