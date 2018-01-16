package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.ExtractionUtils.XmlExtractionUtils._
import dpla.ingestion3.mappers.{ExtractionMapper, IdMinter}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.NodeSeq

// TODO Fixup how shortname is passed down into Extractor
class PaExtractor(shortName: String) extends IdMinter[NodeSeq] with ExtractionMapper[NodeSeq] {

  // Implementation of IdMinter trait methods
  override def useProviderName: Boolean = false

  override def getProviderName: String = shortName

  override def getProviderId(implicit data: NodeSeq): String =
    extractString(data \ "header" \ "identifier")
    .getOrElse[String](throw new RuntimeException(s"No ID for record $data"))


  // SourceResource mapping
  override def collection(data: NodeSeq): Seq[DcmiTypeCollection] =
    extractStrings(data \ "metadata" \\ "relation").headOption.map(nameOnlyCollection).toSeq

  override def contributor(data: NodeSeq): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "contributor").dropRight(1).map(nameOnlyAgent)

  override def creator(data: NodeSeq): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "creator").map(nameOnlyAgent)

  override def date(data: NodeSeq): Seq[EdmTimeSpan] =
    extractStrings(data \ "metadata" \\ "date").distinct.map(stringOnlyTimeSpan)

  override def description(data: NodeSeq): Seq[String] =
    extractStrings(data \ "metadata" \\ "description")

  override def format(data: NodeSeq): Seq[String] =
    extractStrings(data \ "metadata" \\ "type").distinct.filterNot(isDcmiType)

  override def genre(data: NodeSeq): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "type").distinct.map(nameOnlyConcept)

  override def identifier(data: NodeSeq): Seq[String] =
    extractStrings(data \ "metadata" \\ "identifier")

  override def language(data: NodeSeq): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "language").distinct.map(nameOnlyConcept)

  override def place(data: NodeSeq): Seq[DplaPlace] =
    extractStrings(data \ "metadata" \\ "coverage").distinct.map(nameOnlyPlace)

  override def publisher(data: NodeSeq): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "publisher").distinct.map(nameOnlyAgent)

  override def relation(data: NodeSeq): Seq[LiteralOrUri] =
    extractStrings(data \ "metadata" \\ "relation").drop(1).map(eitherStringOrUri)

  override def rights(data: NodeSeq): Seq[String] =
    extractStrings(data \ "metadata" \\ "rights").filter(r => !Utils.isUrl(r))

  override def subject(data: NodeSeq): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject").map(nameOnlyConcept)

  override def title(data: NodeSeq): Seq[String] =
    extractStrings(data \ "metadata" \\ "title")

  override def `type`(data: NodeSeq): Seq[String] =
    extractStrings(data \ "metadata" \\ "type").filter(isDcmiType).map(_.toLowerCase)

  // OreAggregation
  override def dplaUri(data: NodeSeq): URI = mintDplaItemUri()(data)

  override def dataProvider(data: NodeSeq): EdmAgent = {
    val contributors = extractStrings(data \ "metadata" \\ "contributor")
    if (contributors.nonEmpty)
      nameOnlyAgent(contributors.last)
    else
      throw new Exception(s"Missing required property dataProvider because " +
        s"dc:contributor is empty for ${getProviderId(data)}")
  }

  override def edmRights(data: NodeSeq): ZeroToOne[URI] =
    extractStrings(data \ "metadata" \\ "rights").find(r => Utils.isUrl(r)).map(new URI(_))

  override def isShownAt(data: NodeSeq) =
    EdmWebResource(uri = itemUri(data), fileFormat = extractStrings("dc:format")(data))

  override def originalRecord(data: NodeSeq): ExactlyOne[String] = Utils.formatXml(data)

  override def provider(data: NodeSeq): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("PA Digital"),
    uri = Some(new URI("http://dp.la/api/contributor/pa"))
  )

  override def sidecar(data: NodeSeq): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId()(data) )

  /**
    * Extracts the external link to the object from the second occurrence
    * of the dc:identifier property
    *
    * @return URI
    * @throws Exception If dc:identifier does not occur twice
    */
  def itemUri(implicit data: NodeSeq): ExactlyOne[URI] = {
    val ids = extractStrings(data \ "metadata" \\ "identifier")
    if (ids.size >= 2)
      new URI(ids(1))
    else
      throw new Exception(s"dc:identifier does not occur at least twice for: $getProviderId")
  }
}
