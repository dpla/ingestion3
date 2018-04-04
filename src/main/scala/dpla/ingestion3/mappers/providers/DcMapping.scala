package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class DcMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {
  // ID minting functions
  override def useProviderName(): Boolean = false // TODO confirm prefix

  override def getProviderName(): String = "dc" // TODO confirm prefix

  override def getProviderId(implicit data: Document[NodeSeq]): String = // TODO confirm w/gretchen
    extractString(data \ "header" \ "identifier")
      .getOrElse(throw new RuntimeException(s"No ID for record $data")
      )

  // SourceResource mapping
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] = // done
    extractStrings(data \ "metadata" \\ "isPartOf")
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] = // done
    extractStrings(data \ "metadata" \\ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] = // done
    extractStrings(data \ "metadata" \\ "creator")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = // done
    extractStrings(data \ "metadata" \\ "date")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "description") ++
      extractStrings(data \ "metadata" \\ "source")

  override def format(data: Document[NodeSeq]): Seq[String] = // TODO confirm enrichments applied at mapping
    extractStrings(data \ "metadata" \\ "format") ++
      extractStrings(data \ "metadata" \\ "type")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \ "metadata" \\ "place")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "publisher")
      .map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "type") ++
      extractStrings(data \ "metadata" \\ "format")
    .flatMap(_.splitAtDelimiter(";"))


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

  override def isShownAt(data: Document[NodeSeq]): EdmWebResource =
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
    name = Some("District Digital"),
    uri = Some(Utils.createUri("http://dp.la/api/contributor/dc"))
  )
}
