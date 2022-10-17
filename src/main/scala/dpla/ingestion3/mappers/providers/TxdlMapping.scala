package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.NodeSeq

class TxdlMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  // IdMinter methods
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("txdl")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier").map(_.trim)

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "alternative")

  // DONE
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(metadataRoot(data) \ "isPartOf")
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "contributor")
      .map(nameOnlyAgent) // TODO confirm?

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "creator")
      .map(nameOnlyAgent)

  // TODO confirm providedLabel vs dc:date
  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "providedLabel.TimeSpan.providedLabel")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "description")

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "format")
      .filterNot(isDcmiType) // FIXME confirm

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(metadataRoot(data) \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(metadataRoot(data) \ "spatial")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "publisher")
      .map(nameOnlyAgent)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    (metadataRoot(data) \\ "rights").map(r => {
      r.prefix match {
        case "dc" => r.text
        case _ => ""
      }
    }).filter(_.nonEmpty)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = {
    (metadataRoot(data) \\ "rights").map(r => r.prefix match {
      case "edm" => r.text
      case _ => ""
    })
      .filter(_.nonEmpty)
      .map(URI)
  }

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(metadataRoot(data) \ "subject")
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "type")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  // DONE
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "dataProvider")
      .map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(metadataRoot(data) \ "isShownAt")
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(metadataRoot(data) \ "preview")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("Texas Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/txdl"))
  )

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data) )

  override def tags(data: Document[NodeSeq]): ZeroToMany[URI] = Seq(URI("texas"))

  def metadataRoot(data: Document[NodeSeq]): NodeSeq = data \ "dc"
}
