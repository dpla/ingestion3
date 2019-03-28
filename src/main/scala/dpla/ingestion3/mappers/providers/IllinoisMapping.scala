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

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "il"

  // FIXME Should use OAI header / identifier for ID not the first URI in dc:identifier
  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractStrings(data \\ "metadata" \\ "identifier").find(s => HttpUtils.validateUrl(s))

  // SourceResource mapping
  // FIXME Collection information is stored outside of individual records in the OAI setSpec / title
  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    extractStrings(data \\ "metadata" \\ "isPartOf")
      .map(nameOnlyCollection)

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    // alternative
    extractStrings(data \\ "metadata" \\ "alternative")

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // contributor
    extractStrings(data \\ "metadata" \\ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // creator
    extractStrings(data \\ "creator")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // dc:date. If not present use <dcterms:created>
    (if (extractStrings(data \\ "metadata" \\ "date").nonEmpty) {
      extractStrings(data \\ "metadata" \\ "date")
    } else {
      extractStrings(data \\ "metadata" \\ "created")
    }).map(stringOnlyTimeSpan)


  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
  // description
    extractStrings(data \\ "metadata" \\ "description")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
  // format ++ medium
    (extractStrings(data \\ "metadata" \\ "format") ++ extractStrings(data \\ "metadata" \\ "medium"))
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
  // language
    extractStrings(data \\ "metadata" \\ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
  // spatial
    extractStrings(data \\ "metadata" \\ "spatial")
      .map(nameOnlyPlace)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
  // rights
    extractStrings(data \\ "metadata" \\ "rights")

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
  // subject
    extractStrings(data \\ "metadata" \\ "subject")
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // temporal
    extractStrings(data \\ "metadata" \\ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): ZeroToMany[String] =
  // title
    extractStrings(data \\ "metadata" \\ "title")

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
  // type
    extractStrings(data \\ "metadata" \\ "type")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // provenance
  extractStrings(data \\ "metadata" \\ "provenance")
    .map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // isShownAt
    extractStrings(data \\ "metadata" \\ "isShownAt")
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // preview
    extractStrings(data \\ "metadata" \\ "preview")
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
