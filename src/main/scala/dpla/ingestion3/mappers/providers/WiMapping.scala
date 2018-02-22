package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class WiMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {
  // ID minting functions
  // TODO confirm WI does not use prefix.
  override def useProviderName(): Boolean = false

  override def getProviderId(implicit data: Document[NodeSeq]): String =
    extractString(data \ "header" \ "identifier")
      .getOrElse(throw new RuntimeException(s"No ID for record $data")
      )

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): Seq[String] = extractStrings(data \ "metadata" \\ "alternative")

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(data \ "metadata" \\ "isPartOf").headOption.map(nameOnlyCollection).toSeq

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "contributor").dropRight(1).map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "creator").map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(data \ "metadata" \\ "date").map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "description")

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "format").filterNot(isDcmiType) ++
      extractStrings(data \ "metadata" \\ "medium").filterNot(isDcmiType)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "language").map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \ "metadata" \\ "spatial").map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "publisher").map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): Seq[LiteralOrUri] =
    extractStrings(data \ "metadata" \\ "relation").map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "rights") ++
      extractStrings(data \ "metadata" \\ "accessRights")

  override def rightsHolder(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "rightsHolder").map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject").map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "type").filter(isDcmiType)


  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): EdmAgent = {
    extractString(data \ "metadata" \\ "dataProvider") match {
      case Some(provider) => nameOnlyAgent(provider)
      case None => throw new Exception("Missing required property dataProvider")
    }
  }

  override def edmRights(data: Document[NodeSeq]): ZeroToOne[URI] =
    extractStrings(data \ "metadata" \\ "rights").find(r => Utils.isUrl(r)).map(new URI(_))

  // TODO Catch unmintable URI
  override def isShownAt(data: Document[NodeSeq]): EdmWebResource = {
    extractString(data \ "metadata" \\ "isShownAt") match {
      case Some(uri) => uriOnlyWebResource(new URI(uri))
      case None => throw new Exception("Missing required property isShownAt")
    }
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  // TODO Catch unmintable URI
  override def preview(data: Document[NodeSeq]): ZeroToOne[EdmWebResource] =
    extractString(data \ "metadata" \\ "preview").map(x => uriOnlyWebResource(new URI(x)))

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))


  def agent = EdmAgent(
    name = Some("Recollection Wisconsin"),
    uri = Some(new URI("http://dp.la/api/contributor/wisconsin"))
  )
}
