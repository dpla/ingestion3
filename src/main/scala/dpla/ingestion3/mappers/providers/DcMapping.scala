
package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlMapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class DcMapping extends XmlMapping with XmlExtractor {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName: Boolean = false // TODO confirm prefix

  override def getProviderName: Option[String] = Some("dc") // TODO confirm prefix

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

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

  override def format(data: Document[NodeSeq]): Seq[String] =
    (extractStrings(data \ "metadata" \\ "format") ++
      extractStrings(data \ "metadata" \\ "type"))
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \ "metadata" \\ "Place")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \\ "publisher")
      .map(nameOnlyAgent)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    (data \ "metadata" \\ "rights").flatMap(r => {
      r.prefix match {
        case "dc" => Option(r.text)
        case _ => None
      }
    })

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \\ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    (extractStrings(data \ "metadata" \\ "type") ++
      extractStrings(data \ "metadata" \\ "format"))
      .flatMap(_.splitAtDelimiter(";"))


  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "dataProvider")
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = {
    (data \ "metadata" \\ "rights").flatMap(r => r.prefix match {
      case "edm" => Some(URI(r.text))
      case _ => None
    })
  }

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "isShownAt").map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "preview")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("District Digital"),
    uri = Some(URI("http://dp.la/api/contributor/dc"))
  )
}