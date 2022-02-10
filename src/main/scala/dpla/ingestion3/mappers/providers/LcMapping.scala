package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

class LcMapping() extends JsonMapping with JsonExtractor {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName: Boolean = false

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: Option[String] = Some("loc")

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString(unwrap(data) \ "item" \ "id") // TODO confirm basis field for DPLA ID

  // OreAggregation fields
  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \\ "repository")
      .flatMap {
        case s if s.startsWith("Library of Congress") => Some("Library of Congress")
        case s if s.startsWith("Library of Virginia Richmond, VA") => Some("Library of Virginia")
        case s if s.startsWith("Virginia Historical Society") => Some("Virginia Historical Society")
        case _ => None
      }
      .map(nameOnlyAgent)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "item" \ "resource" \ "image")
      .map(stringOnlyWebResource)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("Library of Congress"),
    uri = Some(URI("http://dp.la/api/contributor/lc"))
  )

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
  // item['url']
    extractStrings(unwrap(data) \ "item" \ "url").map(stringOnlyWebResource)

  // SourceResource
  override def alternateTitle(data: Document[JValue]): ZeroToMany[String] = {
    // item['other-title'] OR item['other-titles'] OR item['alternate_title']
    val otherTitle = extractStrings(unwrap(data) \ "item" \ "other-title")
    val otherTitles = extractStrings(unwrap(data) \ "item" \ "other-titles")
    val alternateTitle = extractStrings(unwrap(data) \ "item" \ "alternate_title")

    (otherTitle.nonEmpty, otherTitles.nonEmpty) match {
      case (true, _) => otherTitle
      case (false, true) => otherTitles
      case _ => alternateTitle
    }
  }

  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
  // [partof['title'] for partof in item['partof']
    extractStrings(unwrap(data) \\ "partof" \ "title")
      .map(nameOnlyCollection)

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] = {
    // item['contributor_names'] OR name in item['contributors']]
    val contributorNames = extractStrings(unwrap(data) \\ "contributor_names")
    val lcContributors = extractStrings(unwrap(data) \\ "contributors")

    (if (contributorNames.nonEmpty) contributorNames else lcContributors).map(nameOnlyAgent)
  }

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] = {
    // item['date'] OR item['dates']
    val date = extractStrings(unwrap(data) \\ "date")
    val dates = extractStrings(unwrap(data) \\ "dates")

    (if (date.nonEmpty) date else dates).map(stringOnlyTimeSpan)
  }

  override def description(data: Document[JValue]): ZeroToMany[String] =
  // item['description'] AND item['created_published']
    extractStrings(unwrap(data) \ "item" \ "description") ++
      extractStrings(unwrap(data) \ "item" \ "created_published")

  override def extent(data: Document[JValue]): ZeroToMany[String] =
  // item['medium']
    extractStrings(unwrap(data) \ "item" \ "medium")

  override def format(data: Document[JValue]): ZeroToMany[String] = {
    // (item['type'] AND item['genre']) OR type in item['format']],
    val format =
      extractStrings(unwrap(data) \ "item" \ "type") ++
        extractStrings(unwrap(data) \ "item" \ "genre") ++
        extractStrings(unwrap(data) \ "item" \ "format" \ "type")

    format.map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)
  }

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
  // item['id']
    extractStrings(unwrap(data) \ "item" \ "id")

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] = {
    // item['language']].keys
    extractKeys(unwrap(data) \ "item" \ "language").map(nameOnlyConcept)
  }

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
  // item['location']].keys
  // loc.gov/item: item['coordinates']   << lat // TODO How should this integrate?
    extractKeys(unwrap(data) \ "item" \ "location")
      .map(_.capitalizeFirstChar) // capitalize first char since we are using json keys
      .map(nameOnlyPlace)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
  // "For rights relating to this resource, visit " + same mapping for isShownAt
    isShownAt(data).flatMap(edmWr => Seq(s"For rights relating to this resource, visit ${edmWr.uri.value}"))

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
  // item['subject_headings']
    extractStrings(unwrap(data) \ "item" \ "subject_headings").map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] =
  // item['title']
    extractStrings(unwrap(data) \ "item" \ "title")

  override def `type`(data: Document[JValue]): ZeroToMany[String] = {
    // item['type'] OR item['original_format']].keys
    extractStrings(unwrap(data) \ "item" \ "type") ++
      extractKeys(unwrap(data) \ "item" \ "original_format") ++
      format(data)
  }
}

