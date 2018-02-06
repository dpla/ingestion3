package dpla.ingestion3.mappers.providers

import java.net.{URI, URL}

import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonDSL._

import scala.util.Try
import scala.xml._


/**
TODO
1. EDTF enrichment that supports parsing YYYY-YYYY range into begin and end dates

**/


class OhioExtractor(rawData: String, shortName: String) extends Extractor with XmlExtractionUtils {

  implicit val xml: Elem = XML.loadString(rawData)

  private val typeLookup = Map("stillimage" -> "image",
                           "text" -> "text",
                           "image" -> "image",
                           "movingimage" -> "moving image",
                           "physicalobject" -> "physical object")

  private val formatsToRemove = Seq("jpg",
    "application/pdf",
    "image/jpeg",
    "image/jp2",
    "pdf",
    "video/jpeg",
    "tif",
    "image/tiff",
    "video/jpeg2000",
    "HTML",
    "JPEG2000",
    "text/html",
    "audio/mpeg",
    "JPEG 2000",
    "image/jpg",
    "jpeg2000",
    "charset=UTF-8",
    "charset=utf-8",
    "mp3",
    "video/mp4",
    "video/mpeg",
    "PDF",
    "image/png",
    "jpeg",
    "text/pdf")

  // ID minting functions
  override def useProviderName(): Boolean = false

  override def getProviderName(): String = shortName

  override def getProviderId(): String = extractString(xml \ "header" \ "identifier")
    .getOrElse(throw ExtractorException(s"No ID for record $xml")
  )

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

  def extractIsShownAt(): ExactlyOne[_root_.dpla.ingestion3.model.EdmWebResource] = {
    uriOnlyWebResource(
      new URI(extractString(xml \ "metadata" \\ "isShownAt")
        .getOrElse(throw new RuntimeException("No isShownAt"))))
  }

  def mapDcRights(): AtLeastOne[String] = {
    if (extractEdmRights().isEmpty)
      extractDcRights()
    else
      Seq() // Rights expects AtLeastOne ...
  }

  def extractType(): ZeroToMany[String] = extractStrings(xml \ "metadata" \\ "type")
    .map(t => typeLookup.getOrElse(t.toLowerCase,"")).filter(_.nonEmpty)

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
          .flatMap(_.split(";")) // split around semi-colons
          .map(findAndRemoveAll(_, formatsToRemove)) // remove invalid values (application/pdf MIME type etc.)
          .filter(_.nonEmpty) // Remove empty strings
          .map(capitalizeFirstChar),   // Capitalize the first alpha character
        identifier = extractStrings(xml \ "metadata" \\ "identifier"),
        language = extractStrings(xml \ "metadata" \\ "language")
          .flatMap(_.split(";"))
          .map(_.trim)
          .filter(_.isEmpty)
          .map(nameOnlyConcept),
        place = extractStrings(xml \ "metadata" \\ "spatial")
          .flatMap(_.split(";"))
          .map(nameOnlyPlace), // FIXME duplicate empty rows in rpt on values like "Lake Erie; "
        publisher = extractStrings(xml \ "metadata" \\ "publisher").map(nameOnlyAgent),
        relation = extractStrings(xml \ "metadata" \\ "relation").map(eitherStringOrUri),
        rights = mapDcRights(),
        rightsHolder = extractStrings(xml \ "metadata" \\ "rightsHolder").map(nameOnlyAgent),
        subject = extractStrings(xml \ "metadata" \\ "subject")
          .flatMap(_.split(";"))
          .filter(_.nonEmpty)
          .map(capitalizeFirstChar)
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
      preview = thumbnail()
    )
  }

  def agent = EdmAgent(
    name = Some("Ohio Digital Network"),
    uri = Some(new URI("http://dp.la/api/contributor/ohio"))
  )

  // Get the last occurrence of the identifier property, there
  // must be at least three dc:identifier properties for there
  // to be a thumbnail
  def thumbnail(): ZeroToOne[EdmWebResource] = {
    val ids = extractStrings(xml \ "metadata" \\ "preview")
    if (ids.nonEmpty)
      Option(uriOnlyWebResource(new URI(ids.head)))
    else
      None
  }

  def extractDataProvider(): ExactlyOne[EdmAgent] = {
    val contributors = extractStrings(xml \ "metadata" \\ "dataProvider")
    if (contributors.nonEmpty)
      nameOnlyAgent(contributors.head)
    else
      throw new Exception(s"Missing required property dataProvider because " +
        s"dataProvider is empty for ${getProviderId()}")
  }

  def isUrl(url: String): Boolean = Try {new URL(url) }.isSuccess




  // These methods to be moved up into StringUtils or StringEnrichments at some point
  // FIXME cleanup methods
  // FIXME What methods should this be run against? Subject and Format
  import util.control.Breaks._

  /**
    *
    * @param str
    * @return
    */
  def capitalizeFirstChar(str: String): String = {
    val charIndex = findFirstChar(str)
    replaceCharAt(str, charIndex, str.charAt(charIndex).toUpper )
  }

  def replaceCharAt(s: String, pos: Int, c: Char): String =
    s.substring(0, pos) + c + s.substring(pos + 1)

  def findFirstChar(str: String): Int = {
    var iter = 0
    breakable {
      str.foreach(chr => {
        if(chr.isLetter) break
        else iter = iter + 1
      })
    }
    iter
  }

  def findAndRemoveAll(string: String,
                       targets: Seq[String]): String = {
    var procStr = string // tmp string
    for(target <- targets) {
      procStr = procStr.replaceAll(target, "")
    }
    procStr
  }
}
