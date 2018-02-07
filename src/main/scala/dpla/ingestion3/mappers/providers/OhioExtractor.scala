package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.StringUtils._
import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonDSL._

import scala.util.Try
import scala.xml._


class OhioExtractor(rawData: String, shortName: String) extends Extractor with XmlExtractionUtils {

  implicit val xml: Elem = XML.loadString(rawData)

  // These values will be stripped out of the format field
  // FIXME Regex to ignore/include punctuation?
  private val formatsToRemove = Set("jpg", "application/pdf", "image/jpeg", "image/jp2", "pdf", "video/jpeg",
    "tif", "image/tiff", "video/jpeg2000", "HTML", "JPEG2000", "text/html", "audio/mpeg", "JPEG 2000", "image/jpg",
    "jpeg2000", "charset=UTF-8", "charset=utf-8", "mp3", "video/mp4", "video/mpeg", "PDF", "image/png", "jpeg",
    "text/pdf")

  // ID minting functions
  override def useProviderName(): Boolean = false

  override def getProviderName(): String = shortName

  override def getProviderId(): String = extractString(xml \ "header" \ "identifier")
    .getOrElse(throw ExtractorException(s"No ID for record $xml")
  )

  def build(): Try[OreAggregation] = Try {
    OreAggregation(
      dplaUri = mintDplaItemUri(),
      sidecar = ("prehashId", buildProviderBaseId()) ~
                ("dplaId", mintDplaId()),
      sourceResource = DplaSourceResource(
        alternateTitle = extractStrings(xml \ "metadata" \\ "alternative"),
        // This method of using NodeSeq is required because of namespace issues.
        collection = extractStrings(xml \ "metadata" \\ "isPartOf").headOption.map(nameOnlyCollection).toSeq,
        contributor = extractStrings(xml \ "metadata" \\ "contributor").map(nameOnlyAgent),
        creator = extractStrings(xml \ "metadata" \\ "creator").map(nameOnlyAgent),
        date = extractStrings(xml \ "metadata" \\ "date")
          .flatMap(_.splitAtDelimiter(";"))
          .map(stringOnlyTimeSpan),
        description = extractStrings(xml \ "metadata" \\ "description"),
        extent = extractStrings(xml \ "metadata" \ "extent"),     // FIXME nothing mapped, no data?
        format = extractStrings(xml \ "metadata" \\ "format")
          .flatMap(_.splitAtDelimiter(";"))
          .map(_.findAndRemoveAll(formatsToRemove))
          .filter(_.nonEmpty)
          .map(_.capitalizeFirstChar),
        identifier = extractStrings(xml \ "metadata" \\ "identifier"),
        language = extractStrings(xml \ "metadata" \\ "language")
          .flatMap(_.splitAtDelimiter(";"))
          .map(nameOnlyConcept),
        place = extractStrings(xml \ "metadata" \\ "spatial")
          .flatMap(_.split(";"))
          .map(nameOnlyPlace),
        publisher = extractStrings(xml \ "metadata" \\ "publisher").map(nameOnlyAgent),
        relation = extractStrings(xml \ "metadata" \\ "relation").map(eitherStringOrUri),
        rights = extractDcRights(),
        rightsHolder = extractStrings(xml \ "metadata" \\ "rightsHolder").map(nameOnlyAgent),
        subject = extractStrings(xml \ "metadata" \\ "subject")
          .flatMap(_.splitAtDelimiter(";"))
          .map(_.capitalizeFirstChar)
          .map(nameOnlyConcept),
        title = extractStrings(xml \ "metadata" \\ "title"),
        `type` = extractStrings(xml \ "metadata" \\ "type")
      ),
      dataProvider = extractDataProvider(), // required
      edmRights = extractEdmRights(),
      originalRecord = Utils.formatXml(xml),
      provider = agent,
      isShownAt = extractIsShownAt(),
      preview = extractStrings(xml \ "metadata" \\ "preview")
        .map(uri => uriOnlyWebResource(createUri(uri)))
        .headOption
    )
  }

  def agent = EdmAgent(
    name = Some("Ohio Digital Network"),
    uri = Some(createUri("http://dp.la/api/contributor/ohio"))
  )

  def extractDataProvider(): ExactlyOne[EdmAgent] = {
    val contributors = extractStrings(xml \ "metadata" \\ "dataProvider")
    if (contributors.nonEmpty)
      nameOnlyAgent(contributors.head)
    else
      throw new Exception(s"Missing required property metadata/dataProvider is empty for ${getProviderId()}")
  }

  def extractDcRights(): ZeroToMany[String] = {
    (xml \ "metadata" \\ "rights").map(r => r.prefix match {
      case "dc" => r.text
      case _ => ""
    }).filter(_.isEmpty)
  }

  def extractEdmRights(): ZeroToOne[URI] = {
    (xml \ "metadata" \\ "rights").map(r => r.prefix match {
      case "edm" => createUri(r.text)
    }).headOption
  }

  def extractIsShownAt(): ExactlyOne[EdmWebResource] = {
    uriOnlyWebResource(
      createUri(extractString(xml \ "metadata" \\ "isShownAt")
        .getOrElse(throw new RuntimeException(s"No isShownAt property in record ${getProviderId()}"))))
  }
}
