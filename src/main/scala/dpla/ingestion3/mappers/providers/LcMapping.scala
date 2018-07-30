package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, IdMinter, JsonExtractor, Mapping}
import dpla.ingestion3.messages.{IngestErrors, IngestMessage, MessageCollector}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class LcMapping() extends Mapping[JValue] with IdMinter[JValue] with JsonExtractor with IngestErrors {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName: Boolean = false

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: String =
    "loc"

  override def getProviderId(implicit data: Document[JValue]): String =
    extractString(unwrap(data) \ "item" \ "id") // TODO confirm basis field for DPLA ID
      .getOrElse(throw new RuntimeException(s"No ID for record: ${compact(data)}"))

  // OreAggregation fields
  override def dplaUri(data: Document[JValue]): ExactlyOne[URI] =
    mintDplaItemUri(data)

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dataProvider(data: Document[JValue]) (implicit msgCollector: MessageCollector[IngestMessage]): ExactlyOne[EdmAgent] = {
    val dps = extractStrings(unwrap(data) \\ "repository")
    val dpName = dps.headOption match {
      case Some(dp) => {
        dp match {
          case s if s.startsWith("Library of Congress") => "Library of Congress"
          case s if s.startsWith("Library of Virginia Richmond, VA") => "Library of Virginia"
          case s if s.startsWith("Virginia Historical Society") => "Virginia Historical Society"
          case _ =>
            val dpError = if(dp.isEmpty) "<EMPTY STRING>" else dp
            throw new RuntimeException(s"Record ${getProviderId(data)} " +
              s"has invalid 'repository' value: $dpError")
        }
      }
      case _ => throw new RuntimeException("Missing required property " +
        "'repository' for dataProvider mapping")
    }
    nameOnlyAgent(dpName)
  }

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToOne[EdmWebResource] =
    extractStrings(unwrap(data) \ "item" \ "resource" \ "image")
      .headOption.map(uri => uriOnlyWebResource(new URI(uri)))

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("Library of Congress"),
    uri = Some(new URI("http://dp.la/api/contributor/lc"))
  )

  override def isShownAt(data: Document[JValue]): ExactlyOne[EdmWebResource] =
    // item['url']
    uriOnlyWebResource(providerUri(data))

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
      extractStrings(unwrap(data) \ "item" \ "created_published" )

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
    Seq(s"For rights relating to this resource, visit ${providerUri(data).toString}")

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

  // Helper methods
  def providerUri(json: JValue): URI =
    // item['url']
    extractString(json \ "item" \ "url") match {
      case Some(url) => new URI(url)
      case None => throw new RuntimeException("Missing required property 'url' for 'isShownAt' mapping ")
    }
}
