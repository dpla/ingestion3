package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList}
import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.util.{Failure, Success, Try}
import scala.xml._

class WiMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      ExtentIdentificationList.termList

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
      (data \ "metadata" \\ "accessRights")).map(rights => {
        rights.prefix match {
          case "dc" => rights.text
          case _ => ""
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
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): EdmAgent = {
    extractString(data \ "metadata" \\ "dataProvider") match {
      case Some(provider) => nameOnlyAgent(provider)
      case None => throw new Exception(s"Record ${getProviderId(data)} is missing required property dataProvider")
    }
  }

  override def edmRights(data: Document[NodeSeq]): ZeroToOne[URI] = {
    // Will quietly drop any invalid URIs
    val edmRights = (data \ "metadata" \\ "rights").map(rights => {
      rights.prefix match {
        case "edm" => rights.text
        case _ => ""
      }
    })

    edmRights.find(Utils.isUri).map(new URI(_))
  }

  override def isShownAt(data: Document[NodeSeq]): EdmWebResource = {
    extractString(data \ "metadata" \\ "isShownAt") match {
      case Some(uri) =>
        if(Utils.isUri(uri)) {
          uriOnlyWebResource(new URI(uri))
        } else { throw new RuntimeException(s"Error mapping required property isShownAt in record " +
            s"${getProviderId(data)}. Unable to mint URI from '$uri'")
        }
      case None => throw new RuntimeException(s"Record ${getProviderId(data)} is missing required property isShownAt")
    }
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToOne[EdmWebResource] =
    extractString(data \ "metadata" \\ "preview").map(x =>
      Try(new URI(x)) match {
        case Success(u) => uriOnlyWebResource(u)
        case Failure(f) => throw new RuntimeException(s"Error mapping preview in record ${getProviderId(data)}. " +
          s"Unable to mint URI from '$x': ${f.getMessage}")
      }
      // uriOnlyWebResource(x)
    )

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))


  def agent = EdmAgent(
    name = Some("Recollection Wisconsin"),
    uri = Some(new URI("http://dp.la/api/contributor/wisconsin"))
  )
}
