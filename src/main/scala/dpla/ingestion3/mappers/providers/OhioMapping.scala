package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import dpla.ingestion3.enrichments.StringUtils._
import dpla.ingestion3.enrichments.filters.DigitalSurrogateBlockList
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class OhioMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {
  // ID minting functions
  override def useProviderName(): Boolean = false

  override def getProviderName(): String = "ohio"

  override def getProviderId(implicit data: Document[NodeSeq]): String =
    extractString(data \ "header" \ "identifier")
      .getOrElse(throw new RuntimeException(s"No ID for record $data")
      )

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \\ "alternative")

  // Only use the first isPartOf instance
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(data \ "metadata" \\ "isPartOf")
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "creator")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(data \ "metadata" \\ "date")
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "description")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \\ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "format")
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.applyFilter(DigitalSurrogateBlockList.termList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "language")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \ "metadata" \\ "spatial")
      .flatMap(_.split(";"))
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): Seq[LiteralOrUri] =
    extractStrings(data \ "metadata" \\ "relation")
      .map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    (data \ "metadata" \\ "rights").map(r => {
      r.prefix match {
        case "dc" => r.text
        case _ => ""
      }
    }).filter(_.nonEmpty)

  override def rightsHolder(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "rightsHolder")
      .map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.capitalizeFirstChar)
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "type")


  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): EdmAgent = {
    extractStrings(data \ "metadata" \\ "dataProvider")
      .map(nameOnlyAgent)
      .headOption // take the first value
      .getOrElse( // return the first value or throw an exception
      throw new Exception(s"Missing required property metadata/dataProvider is empty for ${getProviderId(data)}")
    )
  }

  override def edmRights(data: Document[NodeSeq]): ZeroToOne[URI] = {
    (data \ "metadata" \\ "rights").map(r => r.prefix match {
      case "edm" => Utils.createUri(r.text)
    }).headOption
  }

  override def isShownAt(data: Document[NodeSeq]) =
    uriOnlyWebResource(
      Utils.createUri(extractStrings(data \ "metadata" \\ "isShownAt")
        .headOption
        .getOrElse(
          throw new RuntimeException(s"No isShownAt property in record ${getProviderId(data)}")
        )))

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToOne[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "preview")
      .map(uri => uriOnlyWebResource(Utils.createUri(uri)))
      .headOption

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Ohio Digital Network"),
    uri = Some(Utils.createUri("http://dp.la/api/contributor/ohio"))
  )
}
