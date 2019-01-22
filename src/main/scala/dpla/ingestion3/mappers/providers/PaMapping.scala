package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

class PaMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq]
  with IngestMessageTemplates {

  // IdMinter methods
  override def useProviderName: Boolean = false

  // getProviderName is not implemented here because useProviderName is false

  // TODO Add message collect here
  override def getProviderId(implicit data: Document[NodeSeq]): String =
    extractString(data \ "header" \ "identifier")
      .getOrElse[String](throw new RuntimeException(s"No ID for record $data"))

  // SourceResource mapping
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(data \ "metadata" \\ "relation").headOption.map(nameOnlyCollection).toSeq

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "contributor").dropRight(1).map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "creator").map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(data \ "metadata" \\ "date").distinct.map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "description")

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "type").distinct.filterNot(isDcmiType)

  override def genre(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "type").distinct.map(nameOnlyConcept)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "language").distinct.map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \ "metadata" \\ "coverage").distinct.map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "publisher").distinct.map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): Seq[LiteralOrUri] =
    extractStrings(data \ "metadata" \\ "relation").drop(1).map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "rights").filter(r => !Utils.isUrl(r))

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject").map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "type").filter(isDcmiType).map(_.toLowerCase)


  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def originalId(data: Document[NodeSeq]): ExactlyOne[String] = getProviderId(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    extractStrings(data \ "metadata" \\ "contributor").lastOption match {
      case Some(lastContributor) => Seq(nameOnlyAgent(lastContributor))
      case None => Seq()
    }
  }

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    extractStrings(data \ "metadata" \\ "rights").find(r => Utils.isUrl(r)).map(URI).toSeq

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
    extractStrings(data \ "metadata" \\ "source").map(nameOnlyAgent).headOption

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    Try { extractStrings(data \ "metadata" \\ "identifier")(1) } match {
      case Success(t) =>
        Seq(EdmWebResource(uri = URI(t), fileFormat = extractStrings("dc:format")(data)))
      case Failure(_) => Seq()
    }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = thumbnail(data).toSeq

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("PA Digital"),
    uri = Some(URI("http://dp.la/api/contributor/pa"))
  )

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data) )

/**
    *  Get the last occurrence of the identifier property, there
    *  must be at least three dc:identifier properties for there
    *  to be a thumbnail
    *
    * @param data
    * @return
    */

  def thumbnail(implicit data: Document[NodeSeq]): ZeroToOne[EdmWebResource] = {
    val ids = extractStrings(data \ "metadata" \\ "identifier")
    if (ids.size > 2)
      Option(uriOnlyWebResource(URI(ids.last)))
    else
      None
  }
}
