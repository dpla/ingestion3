package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import dpla.ingestion3.enrichments.StringUtils._
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class OhioMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {
  // These values will be stripped out of the format field
  // FIXME Regex to ignore/include punctuation?
  private val formatsToRemove = Set("application/pdf", "audio/mpeg", "charset=ISO-8859-1", "charset=iso-8859-1",
    "charset=UTF-8", "charset=utf-8", "charset=windows-1252", "HTML", "image/jp2", "image/jpeg", "image/jpg",
    "image/png", "image/tiff", "JPEG 2000", "jpeg", "jpeg2000", "JPEG2000", "jpg", "mp3", "PDF", "pdf", "text/html",
    "text/pdf", "tif", "video/jpeg", "video/jpeg2000", "video/mp4", "video/mpeg")

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

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(data \ "metadata" \\ "isPartOf").headOption.map(nameOnlyCollection).toSeq

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "contributor").map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "creator").map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(data \ "metadata" \\ "date")
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "description")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "format")
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.findAndRemoveAll(formatsToRemove))
      .filter(_.nonEmpty)
      .map(_.capitalizeFirstChar)

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
    extractStrings(data \ "metadata" \\ "publisher").map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): Seq[LiteralOrUri] =
    extractStrings(data \ "metadata" \\ "relation").map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    (data \ "metadata" \\ "rights").map(r => {
      r.prefix match {
        case "dc" => r.text
        case _ => ""
      }
    }).filter(_.isEmpty)

  override def rightsHolder(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "rightsHolder").map(nameOnlyAgent)

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
    val contributors = extractStrings(data \ "metadata" \\ "dataProvider")
    if (contributors.nonEmpty)
      nameOnlyAgent(contributors.head)
    else
      throw new Exception(s"Missing required property metadata/dataProvider is empty for ${getProviderId(data)}")
  }

  override def edmRights(data: Document[NodeSeq]): ZeroToOne[URI] = {
    (data \ "metadata" \\ "rights").map(r => r.prefix match {
      case "edm" => Utils.createUri(r.text)
    }).headOption
  }

  override def isShownAt(data: Document[NodeSeq]) =
    uriOnlyWebResource(
      Utils.createUri(extractString(data \ "metadata" \\ "isShownAt")
        .getOrElse(throw new RuntimeException(s"No isShownAt property in record ${getProviderId(data)}"))))

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToOne[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "preview")
      .map(uri => uriOnlyWebResource(Utils.createUri(uri)))
      .headOption

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))
  def agent = EdmAgent(
    name = Some("Ohio Digital Network"),
    uri = Some(Utils.createUri("http://dp.la/api/contributor/ohio"))
  )
}
