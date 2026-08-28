package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.TaggingUtils._
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

class MwdlMapping extends JsonMapping with JsonExtractor {

  override val enforceDuplicateIds: Boolean = false

  // ID minting
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("mwdl")

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractStrings(unwrap(data) \ "pnx" \ "control" \ "recordid").headOption

  // SourceResource mapping
  override def collection(data: Document[JValue]): Seq[DcmiTypeCollection] =
    // Real Primo VE records use addtitle for collection/series name; lfc01 is absent
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "addtitle")
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
      .map(collapseYearRange)

  override def description(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "description")
      .map(_.limitCharacters(1000))

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds05")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "format")

  override def identifier(data: Document[JValue]): Seq[String] =
    parsedIdentifiers(data)

  override def language(data: Document[JValue]): Seq[SkosConcept] =
    // facets.language is absent in real records; use display.language
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): Seq[DplaPlace] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds08")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyPlace)

  override def relation(data: Document[JValue]): ZeroToMany[LiteralOrUri] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "relation")
      .flatMap(_.splitAtDelimiter(";"))
      .map(eitherStringOrUri)

  override def rights(data: Document[JValue]): ZeroToMany[String] =
    rightsValues(data).filterNot(isRightsUri)

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

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] = {
    val fromLds03 = extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds03")
    if (fromLds03.nonEmpty)
      fromLds03.map(nameOnlyAgent)
    else
      // Primo VE stores the institution name in electronicServices.packageName
      // as "Display resource from <Institution Name>" — strip the prefix.
      extractStrings(
        unwrap(data) \ "delivery" \ "electronicServices" \ "packageName"
      ).map(_.replaceFirst("(?i)^display resource from ", "").trim)
        .filter(_.nonEmpty)
        .distinct
        .map(nameOnlyAgent)
  }

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    (rightsValues(data) ++
      extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds13"))
      .filter(isRightsUri)
      .distinct
      .map(URI)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] = {
    val fromDelivery =
      extractStrings(unwrap(data) \ "delivery" \ "availabilityLinksUrl")
        .filter(_.nonEmpty)
    if (fromDelivery.nonEmpty)
      fromDelivery.map(stringOnlyWebResource)
    else {
      // Fallback 1: lds10 contains the original item URL in real Primo VE records
      val fromLds10 = extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds10")
        .filter(_.startsWith("http"))
      if (fromLds10.nonEmpty)
        fromLds10.map(stringOnlyWebResource)
      else
        // Fallback 2: parsed identifiers that look like URLs
        parsedIdentifiers(data).filter(_.startsWith("http")).map(stringOnlyWebResource)
    }
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

  /** Collapses a semicolon-delimited list of individual years (as Primo
    * sometimes expands date ranges) into a single "begin-end" string.
    * If the parts are not all 4-digit years, returns the original string
    * unchanged.
    */
  private def collapseYearRange(raw: String): EdmTimeSpan = {
    val parts = raw.split(";").map(_.trim).filter(_.nonEmpty)
    val years = parts.collect { case p if p.matches("[0-9]{4}") => p.toInt }
    val sortedDistinct = years.distinct.sorted
    val isContinuousRange =
      years.length > 1 &&
        years.length == parts.length &&
        sortedDistinct.last - sortedDistinct.head + 1 == sortedDistinct.length
    if (isContinuousRange)
      stringOnlyTimeSpan(s"${sortedDistinct.head}-${sortedDistinct.last}")
    else
      stringOnlyTimeSpan(raw)
  }

  private val primoIdentifierRegex = "\\$\\$C[^$]+\\$\\$V".r

  private def parsedIdentifiers(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "identifier")
      .flatMap(_.splitAtDelimiter(";"))
      .map(primoIdentifierRegex.replaceAllIn(_, "").trim)
      .filter(_.nonEmpty)

  private def isRightsUri(v: String): Boolean =
    v.startsWith("http://rightsstatements.org") ||
      v.startsWith("https://rightsstatements.org") ||
      v.startsWith("http://creativecommons.org") ||
      v.startsWith("https://creativecommons.org")

  private def rightsValues(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "rights")
      .flatMap(_.splitAtDelimiter(";"))

  def agent: EdmAgent = EdmAgent(
    name = Some("Mountain West Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/mwdl"))
  )
}
