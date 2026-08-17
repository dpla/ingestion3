package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{
  DigitalSurrogateBlockList,
  FormatTypeValuesBlockList
}
import dpla.ingestion3.enrichments.TaggingUtils._
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

class MwdlMapping extends JsonMapping with JsonExtractor {

  override val enforceDuplicateIds: Boolean = false

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("mwdl")

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractStrings(unwrap(data) \ "pnx" \ "control" \ "recordid").headOption

  // SourceResource mapping
  override def collection(data: Document[JValue]): Seq[DcmiTypeCollection] =
    extractStrings(unwrap(data) \ "pnx" \ "facets" \ "lfc01")
      .map(nameOnlyCollection)

  override def contributor(data: Document[JValue]): Seq[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def creator(data: Document[JValue]): Seq[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "creator")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[JValue]): Seq[EdmTimeSpan] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "creationdate")
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def description(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "description")
      .map(_.limitCharacters(1000))

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds05")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "format")
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "control" \ "recordid")

  override def language(data: Document[JValue]): Seq[SkosConcept] =
    extractStrings(unwrap(data) \ "pnx" \ "facets" \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): Seq[DplaPlace] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds08")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyPlace)

  override def relation(data: Document[JValue]): ZeroToMany[LiteralOrUri] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "relation")
      .flatMap(_.splitAtDelimiter(";"))
      .map(eitherStringOrUri)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "rights")
      .flatMap(_.splitAtDelimiter(";"))

  override def subject(data: Document[JValue]): Seq[SkosConcept] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def temporal(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds09")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "title")

  override def `type`(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "type")
      .map {
        case "text_resource"         => "text"
        case "score"                 => "notated music"
        case "newspaper"             => "periodical"
        case "database"              => "dataset"
        case "dissertation"          => "text"
        case "conference_proceeding" => "text"
        case "reference_entry"       => "text"
        case other                   => other
      }

  // OreAggregation
  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds03")
      .map(nameOnlyAgent)

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds13")
      .filter(v =>
        v.startsWith("http://rightsstatements.org") ||
        v.startsWith("https://rightsstatements.org") ||
        v.startsWith("http://creativecommons.org") ||
        v.startsWith("https://creativecommons.org")
      )
      .map(URI)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] = {
    val fromDelivery =
      extractStrings(unwrap(data) \ "delivery" \ "availabilityLinksUrl")
        .filter(_.nonEmpty)
    if (fromDelivery.nonEmpty)
      fromDelivery.map(stringOnlyWebResource)
    else
      // Fallback: construct URL from record ID using the Primo VE catalog
      extractStrings(unwrap(data) \ "pnx" \ "control" \ "recordid")
        .map(id =>
          s"https://utah-primo.hosted.exlibrisgroup.com/permalink/01UTAH_INST/MWDL/$id"
        )
        .map(stringOnlyWebResource)
  }

  override def iiifManifest(data: Document[JValue]): ZeroToMany[URI] =
    extractStrings(unwrap(data) \ "pnx" \ "links" \ "lln02")
      .map(URI)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    (unwrap(data) \ "delivery" \ "link")
      .filter(node =>
        extractString(node \ "displayLabel")
          .getOrElse("")
          .equalsIgnoreCase("thumbnail")
      )
      .flatMap(node => extractStrings(node \ "linkURL"))
      .map(stringOnlyWebResource)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  override def tags(data: Document[JValue]): ZeroToMany[URI] =
    dataProvider(data)
      .flatMap(p => p.name)
      .flatMap(_.applyNwdhTags)

  def agent: EdmAgent = EdmAgent(
    name = Some("Mountain West Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/mwdl"))
  )
}
