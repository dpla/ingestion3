package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.DigitalSurrogateBlockList
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class InMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("in")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "provenance")
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = {
    val rights = extractStrings(data \ "metadata" \ "qualifieddc" \ "rights")
      .map(_.stripSuffix(";"))
      .map(URI)

    val accessRights = extractStrings(data \ "metadata" \ "qualifieddc" \ "accessRights")
      .filter(_.startsWith("http"))
      .map(_.stripSuffix(";"))
      .map(URI)

    rights ++ accessRights
  }

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
    extractString(data \ "metadata" \ "qualifieddc" \ "mediator")
      .map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "identifier").map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  // SourceResource

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "alternative")

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "creator")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "created")
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "description")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "extent")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] = {
    val medium = extractStrings(data \ "metadata" \ "qualifieddc" \ "medium")
      .flatMap(_.splitAtDelimiter(";"))

    val qdcType = extractStrings(data \ "metadata" \ "qualifieddc" \ "type")
      .flatMap(_.splitAtDelimiter(";"))

    (medium ++ qdcType).map(_.applyBlockFilter(DigitalSurrogateBlockList.termList))
      .filter(_.nonEmpty)
  }

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "identifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "language")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "publisher")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "source")
      .map(stringOnlyWebResource)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "spatial")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyPlace)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "accessRights")
      .filterNot(_.startsWith("http"))

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "temporal")
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "title")

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "type")
      .flatMap(_.splitAtDelimiter(";"))

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  def agent = EdmAgent(
    name = Some("Indiana Memory"),
    uri = Some(URI("http://dp.la/api/contributor/indiana"))
  )
}
