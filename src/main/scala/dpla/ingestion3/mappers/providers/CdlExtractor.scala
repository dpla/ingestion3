package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.json.JsonExtractionUtils
import dpla.ingestion3.model.{DplaSourceResource, EdmWebResource, OreAggregation, _}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import scala.util.Try

class CdlExtractor(rawData: String, shortName: String)
  extends Extractor with JsonExtractionUtils {

  implicit val json: JValue = parse(rawData)

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = shortName

  override def getProviderId(): String = extractString("id")(json)
    .getOrElse(throw ExtractorException(s"No ID for record: ${compact(json)}"))

  def build: Try[OreAggregation] = {
    Try {
      OreAggregation(
        dplaUri = mintDplaItemUri(),
        sidecar = ("prehashId", buildProviderBaseId()) ~
                  ("dplaId", mintDplaId()),
        sourceResource = DplaSourceResource(
          alternateTitle = extractStrings("alternative_title_ss"),
          collection = extractStrings("collection_name").map(nameOnlyCollection),
          contributor = extractStrings("contributor_ss").map(nameOnlyAgent),
          creator = extractStrings("creator_ss").map(nameOnlyAgent),
          date = extractStrings("date_ss").map(stringOnlyTimeSpan),
          description = extractStrings("description_ss"),
          extent = extractStrings("extent_ss"),
          format = extractStrings("format"),
          genre = extractStrings("genre_ss").map(nameOnlyConcept),
          identifier = extractStrings("identifier_ss"),
          language = extractStrings("language_ss").map(nameOnlyConcept),
          place = extractStrings("coverage_ss").map(nameOnlyPlace),
          publisher = extractStrings("publisher_ss").map(nameOnlyAgent),
          relation = extractStrings("relation_ss").map(eitherStringOrUri),
          rights = extractStrings("rights_ss")
            ++ extractStrings("rights_note_ss")
            ++ extractStrings("rights_date_ss"),
          rightsHolder = extractStrings("rightsholder_ss").map(nameOnlyAgent),
          subject = extractStrings("subject_ss").map(nameOnlyConcept),
          temporal = extractStrings("temporal_ss").map(stringOnlyTimeSpan),
          title = extractStrings("title_ss"),
          `type` = extractStrings("type")
        ),
        dataProvider = nameOnlyAgent(provider(json)),
        originalRecord = rawData,
        preview = thumbnail(json),
        provider = agent,
        isShownAt = uriOnlyWebResource(providerUri(json))
      )
    }
  }

  def provider(json: JValue): String = {
    val campus = extractStrings("campus_name")(json).headOption
    val repository = extractStrings("repository_name")(json).headOption
    (campus, repository) match {
      case (Some(campusVal), Some(repositoryVal)) => campusVal + ", " + repositoryVal
      case (None, Some(repositoryVal)) => repositoryVal
      case _ => throw new Exception("Unable to determine provider.")
    }
  }

  def thumbnail(json: JValue): Option[EdmWebResource] =
    extractString("reference_image_md5")(json) match {
      case Some(md5) => Some(
        uriOnlyWebResource(
          new URI("https://thumbnails.calisphere.org/clip/150x150/" + md5)
        )
      )
      case None => None
    }

  def agent = EdmAgent(
    name = Some("California Digital Library"),
    uri = Some(new URI("http://dp.la/api/contributor/cdl"))
  )

  def providerUri(json: JValue): URI =
    extractString("url_item")(json) match {
      case Some(url) => new URI(url)
      case None => throw new Exception("Unable to determine URL of item on provider's site")
    }
}
