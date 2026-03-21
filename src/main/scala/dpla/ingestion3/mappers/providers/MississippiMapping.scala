package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{
  AtLeastOne,
  ExactlyOne,
  ZeroToMany,
  ZeroToOne
}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonDSL._
import org.json4s._

class MississippiMapping
    extends JsonMapping
    with JsonExtractor
    with IngestMessageTemplates {

  val formatBlockList: Set[String] = ExtentIdentificationList.termList

  override val warnMissingEdmRights: Boolean = true

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("mississippi")

  // TODO confirm this is the most stable identifier for these records
  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString(unwrap(data) \ "@id")

  // OreAggregation
  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds02")
      .map(nameOnlyAgent)

  // TODO ask provider to include collection name (e.g. "Canton Data Photographic
  // Collection") in their Primo export — it is displayed on their CDM item pages
  // but does not appear in the harvested PNX data. Map to collection once available.

  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    rightsValues(data).filter(isRightsUri).map(URI)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "delivery" \ "availabilityLinksUrl")
      .map(stringOnlyWebResource)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] = {
    (unwrap(data) \ "delivery" \ "link")
      .filter(node =>
        extractString(node \ "displayLabel")
          .getOrElse("")
          .equalsIgnoreCase("thumbnail")
      )
      .flatMap(node => extractStrings(node \ "linkURL"))
      .map(stringOnlyWebResource)
  }

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "creator")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  // TODO ask provider to move the LSTA funding note out of contributor — all
  // Madison County records have "Digitization of the collection is being
  // partially funded by the LSTA grant..." as a contributor value, which is
  // a funding statement, not a contributing agent.
  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "description")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "format")

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "identifier")
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.replaceAll("\\$\\$C[^$]+\\$\\$V", "").trim)
      .filter(_.nonEmpty)

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] = {
    val fromLds14 = extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds14")
    // coverage mixes temporal values (e.g. "1940s (1940-1949)") and geographic
    // values (e.g. "Canton (Miss.)"). Geographic values are identified by a
    // state abbreviation pattern: word(s) followed by (Abbrev.)
    val fromCoverage = extractStrings(unwrap(data) \ "pnx" \ "display" \ "coverage")
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.matches(".*\\([A-Z][a-z]+\\.\\)"))
    (fromLds14 ++ fromCoverage).distinct.map(nameOnlyPlace)
  }

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "publisher")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def rights(data: Document[JValue]): ZeroToMany[String] =
    rightsValues(data).filterNot(isRightsUri)

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "creationdate")
      .map(collapseYearRange)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "title")

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
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

  /** Collapses a semicolon-delimited list of individual years (as Primo
    * sometimes expands date ranges) into a single "begin-end" string.
    * If the parts are not all 4-digit years, returns the original string
    * unchanged.
    */
  private def collapseYearRange(raw: String): EdmTimeSpan = {
    val parts = raw.split(";").map(_.trim).filter(_.nonEmpty)
    val years = parts.collect { case p if p.matches("[0-9]{4}") => p.toInt }
    if (years.length > 1 && years.length == parts.length) {
      val sorted = years.sorted
      stringOnlyTimeSpan(s"${sorted.head}-${sorted.last}")
    } else
      stringOnlyTimeSpan(raw)
  }

  private def isRightsUri(v: String): Boolean =
    v.startsWith("http://rightsstatements.org") ||
      v.startsWith("https://rightsstatements.org") ||
      v.startsWith("http://creativecommons.org") ||
      v.startsWith("https://creativecommons.org")

  private def rightsValues(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "rights")
      .flatMap(_.splitAtDelimiter(";"))

  def agent: EdmAgent = EdmAgent(
    name = Some("Mississippi Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/mississippi-digital-library"))
  )
}
