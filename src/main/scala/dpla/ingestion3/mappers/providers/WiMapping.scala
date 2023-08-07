package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.TaggingUtils.Tags
import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class WiMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("wisconsin")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "alternative")

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(data \ "metadata" \\ "isPartOf").map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "contributor").dropRight(1).map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "creator").map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    (extractStrings(data \ "metadata" \\ "date") ++
      extractStrings(data \ "metadata" \\ "temporal"))
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "description")
      .map(_.limitCharacters(1000))

  override def extent(data: Document[NodeSeq]): Seq[String] = {
    (extractStrings(data \ "metadata" \\ "format") ++
      extractStrings(data \ "metadata" \\ "medium") ++
      extractStrings(data \ "metadata" \\ "extent"))
      .map(_.applyAllowFilter(ExtentIdentificationList.termList))
      .filter(_.nonEmpty)
  }

  override def format(data: Document[NodeSeq]): Seq[String] = {
    (extractStrings(data \ "metadata" \\ "format") ++
      extractStrings(data \ "metadata" \\ "medium"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)
  }

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
    ((data \ "metadata" \\ "rights") ++
      (data \ "metadata" \\ "accessRights")).flatMap(rights => {
      rights.prefix match {
        case "dc" => Some(rights.text.trim) // dc:rights
        case "dct" => Some(rights.text.trim) // dct:accessRights
        case _ => None
      }
    })

  override def rightsHolder(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "rightsHolder").map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "type").filter(isDcmiType)

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "dataProvider").map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = {
    // Will quietly drop any invalid URIs
    (data \ "metadata" \\ "rights").flatMap(rights => {
      rights.prefix match {
        case "edm" => Some(URI(rights.text.trim))
        case _ => None
      }
    })
  }

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "isShownAt").map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "preview").map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  override def tags(data: Document[NodeSeq]): ZeroToMany[URI] =
    extractStrings(data \ "header" \ "setSpec").flatMap(_.applyWisconsinGovernmentTags)



  def agent = EdmAgent(
    name = Some("Recollection Wisconsin"),
    uri = Some(URI("http://dp.la/api/contributor/wisconsin"))
  )
}