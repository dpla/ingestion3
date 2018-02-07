package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.StringUtils._
import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonDSL._

import scala.util.Try
import scala.xml._


class OhioExtractor(rawData: String, shortName: String) extends Extractor with XmlExtractionUtils {

  implicit val xml: Elem = XML.loadString(rawData)

  // Mapping of Ohio type values to DPLA DCMI type values
  private val typeLookup = Map(
    "image" -> "image",
    "movingimage" -> "moving image",
    "physicalobject" -> "physical object",
    "stillimage" -> "image",
    "text" -> "text"
  )

  // These values will be stripped out of the format field
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

  def build: Try[OreAggregation] = Try {
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
        date = extractStrings(xml \ "metadata" \\ "date").map(stringOnlyTimeSpan),
        description = extractStrings(xml \ "metadata" \\ "description"),
        extent = extractStrings(xml \ "metadata" \ "extent"),     // FIXME nothing mapped, no data?
        format = extractStrings(xml \ "metadata" \\ "format")
          .flatMap(_.splitAtDelimiter(";")) // split around semi-colons
          .map(_.findAndRemoveAll(formatsToRemove)) // remove invalid values (application/pdf MIME type etc.)
          .filter(_.nonEmpty) // Remove empty strings
          .map(_.capitalizeFirstChar),   // Capitalize the first alpha character
        identifier = extractStrings(xml \ "metadata" \\ "identifier"),
        language = extractStrings(xml \ "metadata" \\ "language")
          .flatMap(_.splitAtDelimiter(";"))
          .map(nameOnlyConcept),   // FIXME There should be cleaner / more consistent way of splitting, trimming, filtering
        place = extractStrings(xml \ "metadata" \\ "spatial")
          .flatMap(_.split(";"))
          .map(nameOnlyPlace),
        publisher = extractStrings(xml \ "metadata" \\ "publisher").map(nameOnlyAgent),
        relation = extractStrings(xml \ "metadata" \\ "relation").map(eitherStringOrUri),
        rights = mapDcRights(),
        rightsHolder = extractStrings(xml \ "metadata" \\ "rightsHolder").map(nameOnlyAgent),
        subject = extractStrings(xml \ "metadata" \\ "subject")
          .flatMap(_.splitAtDelimiter(";"))
          .map(_.capitalizeFirstChar)
          .map(nameOnlyConcept),
        title = extractStrings(xml \ "metadata" \\ "title"),
        `type` = extractType()
      ),
      //below will throw if not enough contributors
      dataProvider = extractDataProvider(),
      edmRights = extractEdmRights(),
      originalRecord = Utils.formatXml(xml),
      provider = agent,
      isShownAt = extractIsShownAt(),
      preview = extractThumbnail()
    )
  }

  def agent = EdmAgent(
    name = Some("Ohio Digital Network"),
    uri = Some(new URI("http://dp.la/api/contributor/ohio"))
  )

  def extractDataProvider(): ExactlyOne[EdmAgent] = {
    val contributors = extractStrings(xml \ "metadata" \\ "dataProvider")
    if (contributors.nonEmpty)
      nameOnlyAgent(contributors.head)
    else
      throw new Exception(s"Missing required property dataProvider because " +
        s"metadata/dataProvider is empty for ${getProviderId()}")
  }

  def extractDcRights(): Seq[String] = {
    (xml \ "metadata" \\ "rights").map(r => r.prefix match {
      case "dc" => r.text
    })
  }

  def extractEdmRights(): ZeroToOne[URI] = {
    (xml \ "metadata" \\ "rights").map(r => r.prefix match {
      case "edm" => new URI(r.text)
    }).headOption
  }

  // TODO URL validation besides waiting for Runtime exception on new URI(...)
  def extractIsShownAt(): ExactlyOne[EdmWebResource] = {
    uriOnlyWebResource(
      new URI(extractString(xml \ "metadata" \\ "isShownAt")
        .getOrElse(throw new RuntimeException("No isShownAt"))))
  }

  def extractThumbnail(): ZeroToOne[EdmWebResource] = {
    val ids = extractStrings(xml \ "metadata" \\ "preview")
    if (ids.nonEmpty)
      Option(uriOnlyWebResource(new URI(ids.head)))
    else
      None
  }

  def extractType(): ZeroToMany[String] = extractStrings(xml \ "metadata" \\ "type")
    .map(t => typeLookup
      .getOrElse(t.toLowerCase, throw new RuntimeException(s"No type for record ${getProviderId()}")))
    .filter(_.nonEmpty)

  // FIXME This shouldn't be necessary, why not map both DC and EDM?
  def mapDcRights(): AtLeastOne[String] = {
    if (extractEdmRights().isEmpty)
      extractDcRights()
    else
      Seq() // Rights expects AtLeastOne ...
  }
}
