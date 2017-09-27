package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.json.JsonExtractionUtils
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany}
import dpla.ingestion3.model.{DplaSourceResource, EdmWebResource, OreAggregation, _}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

class MdlExtractor(rawData: String, shortName: String) extends Extractor with JsonExtractionUtils {
  implicit val json: JValue = parse(rawData)

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = shortName

  override def getProviderId(): String = extractString("id")(json)
    .getOrElse(throw ExtractorException(s"No ID for record: ${compact(json)}"))

  def build: Try[OreAggregation] = {
    Try {
      OreAggregation(
        dplaUri = new URI(mintDplaId()),
        sourceResource = DplaSourceResource(
          collection = collection(json \\ "record" \ "sourceResource" \ "collection"),
          contributor = extractStrings(json \\ "record" \ "sourceResource" \ "contributor").map(nameOnlyAgent),
          creator = extractStrings(json \\ "record" \ "sourceResource" \ "creator").map(nameOnlyAgent),
          date = date(json \\ "record" \ "sourceResource" \ "date"),
          description = extractStrings(json \\ "record" \ "sourceResource" \ "description"),
          format = extractStrings(json \\ "record" \ "sourceResource" \ "format"),
          genre = extractStrings(json \\ "record" \ "sourceResource" \ "type").map(nameOnlyConcept),
          language = extractStrings(json \\ "record" \ "sourceResource" \ "language" \ "iso636_3").map(nameOnlyConcept)
            ++ extractStrings(json \\ "record" \ "sourceResource" \ "language" \ "name").map(nameOnlyConcept),
          place = place(json \\ "record" \ "sourceResource" \ "spatial"),
          publisher = extractStrings(json \\ "record" \ "sourceResource" \ "publisher").map(nameOnlyAgent),
          rights = extractStrings(json \\ "record" \ "sourceResource" \ "rights"),
          subject = extractStrings(json \\ "record" \ "sourceResource" \ "subject" \ "name").map(nameOnlyConcept),
          title = extractStrings(json \\ "record" \ "sourceResource" \ "title"),
          `type` = extractStrings(json \\ "record" \ "sourceResource" \ "type")
        ),
        dataProvider = dataProvider(json \\ "record" \ "dataProvider"),
        originalRecord = rawData,
        isShownAt = uriOnlyWebResource(uri(json)),
        preview = thumbnail(json \\ "record" \ "object"),
        provider = agent
      )
    }
  }

  def collection(collection: JValue): ZeroToMany[DcmiTypeCollection] = {
    collection.children.map(c =>
      DcmiTypeCollection(
        title = extractString(c \\ "name"),
        description = extractString(c \\ "description" \ "dc" \ "description")
      ))
  }

  def dataProvider(dataProvider: JValue): ExactlyOne[EdmAgent] = {
    // Data providers come in arrays and as single values here so we extract
    // as a Seq and take the first one
    extractStrings(dataProvider)
      .headOption
      .map(nameOnlyAgent)
      .getOrElse(throw new RuntimeException(s"dataProvider is missing for\t" +
        s"${
          extractString(json \\ "isShownAt")
            .getOrElse(pretty(render(json)))
        }"))
  }

  def date(date: JValue): ZeroToMany[EdmTimeSpan] = {
    date.children.map(d =>
      EdmTimeSpan(
        begin = extractString(d \ "begin"),
        end = extractString(d \ "end"),
        originalSourceDate = extractString(d \ "displayDate")
      ))
  }

  def place(place: JValue): ZeroToMany[DplaPlace] = {
    place.children.map(p =>
      DplaPlace(
        name = extractString(p \\ "name"),
        coordinates = extractString(p \\ "coordinates")
      ))
  }

  def thumbnail(thumbnail: JValue): Option[EdmWebResource] =
    extractString(thumbnail) match {
      case Some(t) => Some(
        uriOnlyWebResource(new URI(t))
      )
      case None => None
    }

  def agent = EdmAgent(
    name = Some("Minnesota Digital Library"),
    uri = Some(new URI("http://dp.la/api/contributor/mdl"))
  )

  def uri(uri: JValue): URI = {
    extractString(uri) match {
      case Some(t) => new URI(t)
      case _ => throw new RuntimeException(s"isShownAt is missing. Cannot map record.")
    }
  }
}
