package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, IdMinter, JsonExtractor, Mapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


// TODO Should there be an implicit unwrapping of JSON values when calling extract*(JValue)

class LcMapping() extends Mapping[JValue] with IdMinter[JValue] with JsonExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: String =
    "loc"

  override def getProviderId(implicit data: Document[JValue]): String =
    extractString(data.get \ "item" \ "id") // TODO confirm basis field for DPLA ID
      .getOrElse(throw new RuntimeException(s"No ID for record: ${compact(data)}"))

  // OreAggregation fields
  override def dplaUri(data: Document[JValue]): ExactlyOne[URI] =
    mintDplaItemUri(data)

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dataProvider(data: Document[JValue]): ExactlyOne[EdmAgent] =
    // item['repository']
    nameOnlyAgent(
      extractStrings(data.get \\ "repository").headOption
        .getOrElse("Missing required property " +
        "'repository' for dataProvider mapping"))

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  // TODO Add test
  override def preview(data: Document[JValue]): ZeroToOne[EdmWebResource] =
    // resource['image'] (First instance)
    // TODO Confirm mapping, "image_url" might be new field
    extractStrings(data.get \ "item" \ "resource" \ "image")
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
    val otherTitle = extractStrings(data.get \ "item" \ "other-title")
    val otherTitles = extractStrings(data.get \ "item" \ "other-titles")
    val alternateTitle = extractStrings(data.get \ "item" \ "alternate_title")

    // This seems stupid
    if (otherTitle.nonEmpty) otherTitle
    else if (otherTitles.nonEmpty) otherTitles
    else alternateTitle
  }

  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    // [partof['title'] for partof in item['partof']
    extractStrings(data.get \\ "partof" \ "title")
      .map(nameOnlyCollection)

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] = {
    // item['contributor_names'] OR name in item['contributors']]
    val contributorNames = extractStrings(data.get \\ "contributor_names")
    val lcContributors = extractStrings(data.get \\ "contributors")

    val contributors =
      if(contributorNames.nonEmpty) contributorNames
      else lcContributors

    contributors.map(nameOnlyAgent)
  }

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] = {
    // item['date'] OR item['dates']
    val date = extractStrings(data.get \\ "date")
    val dates = extractStrings(data.get \\ "dates")

    if (date.nonEmpty) date.map(stringOnlyTimeSpan)
    else dates.map(stringOnlyTimeSpan)
  }

  override def description(data: Document[JValue]): ZeroToMany[String] =
    // item['description'] AND item['created_published']
    extractStrings(data.get \ "item" \ "description") ++
      extractStrings(data.get \ "item" \ "created_published" )

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    // item['medium']
    extractStrings(data.get \ "item" \ "medium")

  override def format(data: Document[JValue]): ZeroToMany[String] = {
    // (item['type'] AND item['genre']) OR type in item['format']],
    val typeGenre = extractStrings(data.get \ "item" \ "type") ++
      extractStrings(data.get \ "item" \ "genre")
    val formatType = extractStrings(data.get \ "item" \ "format" \ "type")

    if (typeGenre.nonEmpty) typeGenre
    else formatType
  }

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    // item['id']
    extractStrings(data.get \ "item" \ "id")

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] = {
    // item['language']].keys
    extractKeys(data.get \ "item" \ "language").map(nameOnlyConcept)
  }

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    // item['location']].keys
    // loc.gov/item: item['coordinates']   << lat // TODO How should this integrate?
    extractKeys(data.get \ "item" \ "location")
      .map(_.capitalizeFirstChar) // capitalize first char since we are using json keys
      .map(nameOnlyPlace)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    // "For rights relating to this resource, visit " + same mapping for isShownAt
    Seq(s"For rights relating to this resource, visit ${providerUri(data).toString}")

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    // item['subject_headings']
    extractStrings(data.get \ "item" \ "subject_headings").map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    // item['title']
    extractStrings(data.get \ "item" \ "title")

  override def `type`(data: Document[JValue]): ZeroToMany[String] = {
    // item['type'] OR item['format']].keys
    val types = extractStrings(data.get \ "item" \ "type")
    val formatKeys = extractKeys(data.get \ "item" \ "format")

    if (types.nonEmpty) types
    else formatKeys
  }

  // Helper methods
  def providerUri(json: JValue): URI =
    // item['url']
    extractString(json \ "item" \ "url") match {
      case Some(url) => new URI(url)
      case None => throw new RuntimeException("Missing required property 'url' for 'isShownAt' mapping ")
    }
}
