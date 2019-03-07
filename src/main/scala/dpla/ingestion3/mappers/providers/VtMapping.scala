package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class VtMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  override def useProviderName: Boolean = false

  override def getProviderName(): String = "vt"

  // TODO: Confirm with provider that this is correct.  Some records have multiple identifiers.
  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractStrings(data \ "identifier").headOption

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  // TODO: There is no clear mapping for isShownAt
  //   Roughly 85% of records have an identifier that starts with "http".
  //   Brief spot-testing suggested that at least some of these resolve to web pages with the full item.
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "identifier")
      .filter(_.startsWith("http"))
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  // TODO: There is no clear mapping for dataProvider
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    Seq().map(nameOnlyAgent)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "creator")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(data \ "date")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "description")

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "format")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \ "coverage")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \ "relation")
      .map(eitherStringOrUri)

  // TODO: there are some records with rights statement URIs and CC text in the "rights" field
  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \ "rights")

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "subject")
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "type")

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  def agent = EdmAgent(
    name = Some("Green Mountain Digital Archive"),
    uri = Some(URI("http://dp.la/api/contributor/vt"))
  )
}
