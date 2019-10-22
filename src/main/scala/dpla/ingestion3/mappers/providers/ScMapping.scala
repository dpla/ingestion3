package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class ScMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "scdl"

  // TODO ID minting will change SCDL IDs
  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)

  // SourceResource mapping

  // done
  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    (metadata(data) \ "isPartOf")
      .flatMap(extractStrings)
      .map(nameOnlyCollection)


  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // contributor
  // done, sbw
    extractStrings(metadata(data) \ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // creator
  // done, sbw
    extractStrings(metadata(data) \ "creator")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // <date>
  // done, sbw
    extractStrings(metadata(data) \ "date")
      .map(stringOnlyTimeSpan)
  
  override def description(data: Document[NodeSeq]): Seq[String] =
  // <description>
  // done, sbw
    extractStrings(metadata(data) \ "description")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
  // <extent>
  // done, sbw
    extractStrings(metadata(data) \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
  // <medium>
  // done, sbw
    extractStrings(metadata(data) \ "medium")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
  // <identifier>
  // done, sbw
    extractStrings(metadata(data) \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
  // language
  // done, sbw
    extractStrings(metadata(data) \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
  // <spatial>
  // done, sbw
    extractStrings(metadata(data) \ "spatial")
      .map(nameOnlyPlace)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
  // source
  // done, sbw
    extractStrings(metadata(data) \ "source")
      .map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
  // rights and accessRights
  // done, sbw
    extractStrings(metadata(data) \ "rights") ++
      extractStrings(metadata(data) \ "accessRights")

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    // subject
    // done, sbw
     extractStrings(metadata(data) \ "subject")
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // <temporal>
  // done, sbw
    extractStrings(metadata(data) \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
  // <title>
  // done, sbw
    extractStrings(metadata(data) \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
  // <type>
  // done, sbw
    extractStrings(metadata(data) \ "type")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // first publisher
  // done, sbw
    extractStrings(metadata(data) \ "publisher")
      .map(nameOnlyAgent)
      .take(1)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // URLs in dc:identifier property
  // done, sbw
    extractStrings(metadata(data) \ "identifier")
      .filter(Utils.isUrl)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // hasFormat
  // done, sbw
    extractStrings(metadata(data) \ "hasFormat")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("South Carolina Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/scdl"))
  )

  protected def metadata(data: Document[NodeSeq]): Document[NodeSeq] = Document(data \ "metadata" \ "qualifieddc")
}
