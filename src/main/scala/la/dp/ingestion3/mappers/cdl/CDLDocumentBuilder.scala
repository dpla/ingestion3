package la.dp.ingestion3.mappers.cdl

import la.dp.ingestion3.mappers.json.la.dp.ingestion3.mappers.utils.JsonExtractionUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._

class CDLDocumentBuilder extends JsonExtractionUtils {

  def buildFromJson(rawData: String): CDLDocument = {

    implicit val json: JValue = parse(rawData)

    val campus = extractStrings("campus_name").headOption
    val repository = extractStrings("repository_name").headOption

    val provider = (campus, repository) match {
      case (Some(campusVal), Some(repositoryVal)) => Some(campusVal + ", " + repositoryVal)
      case (None, Some(repositoryVal)) => Some(repositoryVal)
      case _ => None
    }

    CDLDocument(
      urlItem = extractString("url_item"),
      imageMD5 = extractString("reference_image_md5"),
      titles = extractStrings("title_ss"),
      identifiers = extractStrings("identifier_ss"),
      dates = extractStrings("date_ss"),
      rights = extractStrings("rights_ss"),
      contributors = extractStrings("contributor_ss"),
      creators = extractStrings("creator_ss"),
      collectionNames = extractStrings("collection_name"),
      publishers = extractStrings("publisher_ss"),
      types = extractStrings("type"),
      campus = campus,
      repository = repository,
      provider = provider
    )
  }
}
