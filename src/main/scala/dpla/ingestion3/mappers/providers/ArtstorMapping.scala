package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._
import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._

import scala.xml._

class ArtstorMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  lazy val isShownAtBaseUrl = "https://www.jstor.org/stable/community."

  // ID minting functions
  override def useProviderName(): Boolean = false

  override def getProviderName(): Option[String] = Some("artstor")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // dc:creator
  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \\ "creator")
      .map(nameOnlyAgent)

  // dc:date
  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(data \\ "date")
      .map(stringOnlyTimeSpan)

  // dc:description
  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "description")

  // dc:format ++ dc:type
  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "format") ++
      extractStrings(data \\ "type")

  // head_dc:identifier
  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "identifier")


  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \\ "rights")

  // dc:subject
  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  // dc:format ++ dc:subject
  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    (extractStrings(data \\ "subject") ++
      extractStrings(data \\ "format"))
      .map(stringOnlyTimeSpan)

  // dc:title
  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "title")

  // dc:format
  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "format")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)
  
  // dc:publisher
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \\ "about" \\ "publisher")
      .map(nameOnlyAgent)

  // <identifier type=”hdl>
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "header" \ "identifier")
      .map(id => s"$isShownAtBaseUrl$id")
      .map(stringOnlyWebResource)

  // Old mapping
//    isShownAtStrings(data)
//      .filter(_.startsWith("ADLImageViewer"))
//      .map(_.replaceFirst("ADLImageViewer:", ""))
//      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    isShownAtStrings(data)
      .filter(_.startsWith("Thumbnail"))
      .map(_.replaceFirst("Thumbnail:", ""))
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("ARTstor"),
    uri = Some(URI("http://dp.la/api/contributor/artstor"))
  )

  def isShownAtStrings(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "identifier")
}
