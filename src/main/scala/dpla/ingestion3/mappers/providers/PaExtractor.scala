package dpla.ingestion3.mappers.providers

import java.net.{URI, URL}

import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonDSL._

import scala.util.Try
import scala.xml._

class PaExtractor(rawData: String, shortName: String) extends Extractor with XmlExtractionUtils {

  implicit val xml: Elem = XML.loadString(rawData)

  // ID minting functions
  override def useProviderName(): Boolean = false

  override def getProviderName(): String = shortName

  override def getProviderId(): String = extractString(xml \ "header" \ "identifier")
    .getOrElse(throw ExtractorException(s"No ID for record ${xml}")
  )


  /**
    * Extracts the external link to the object from the second occurrence
    * of the dc:identifier property
    *
    * @return URI
    * @throws Exception If dc:identifier does not occur twice
   */
  def itemUri(): ExactlyOne[URI] = {
    val ids = extractStrings(xml \ "metadata" \\ "identifier")
    if (ids.size >= 2)
      new URI(ids(1))
    else
      throw new Exception(s"dc:identifier does not occur at least twice for: ${getProviderId()}")
  }

  def build: Try[OreAggregation] = Try {
    OreAggregation(
      dplaUri = mintDplaItemUri(),
      sidecar = ("prehashId", buildProviderBaseId()) ~
                ("dplaId", mintDplaId()),
      sourceResource = DplaSourceResource(
        // This method of using NodeSeq is required because of namespace issues.
        collection = extractStrings(xml \ "metadata" \\ "relation").headOption.map(nameOnlyCollection).toSeq,
        contributor = extractStrings(xml \ "metadata" \\ "contributor").dropRight(1).map(nameOnlyAgent),
        creator = extractStrings(xml \ "metadata" \\ "creator").map(nameOnlyAgent),
        date = extractStrings(xml \ "metadata" \\ "date").distinct.map(stringOnlyTimeSpan),
        description = extractStrings(xml \ "metadata" \\ "description"),
        format = extractStrings(xml \ "metadata" \\ "type").distinct.filterNot(isDcmiType),
        genre = extractStrings(xml \ "metadata" \\ "type").distinct.map(nameOnlyConcept),
        identifier = extractStrings(xml \ "metadata" \\ "identifier"),
        language = extractStrings(xml \ "metadata" \\ "language").distinct.map(nameOnlyConcept),
        place = extractStrings(xml \ "metadata" \\ "coverage").distinct.map(nameOnlyPlace),
        publisher = extractStrings(xml \ "metadata" \\ "publisher").distinct.map(nameOnlyAgent),
        relation = extractStrings(xml \ "metadata" \\ "relation").drop(1).map(eitherStringOrUri),
        rights = extractStrings(xml \ "metadata" \\ "rights").filter(r => !isUrl(r)),
        subject = extractStrings(xml \ "metadata" \\ "subject").map(nameOnlyConcept),
        title = extractStrings(xml \ "metadata" \\ "title"),
        `type` = extractStrings(xml \ "metadata" \\ "type").filter(isDcmiType).map(_.toLowerCase)
      ),
      //below will throw if not enough contributors
      dataProvider = extractDataProvider(),
      edmRights = extractStrings(xml \ "metadata" \\ "rights").find(r => isUrl(r)).map(new URI(_)),
      originalRecord = Utils.formatXml(xml),
      provider = agent,
      isShownAt = EdmWebResource(uri = itemUri(), fileFormat = extractStrings("dc:format")),
      preview = thumbnail()
    )
  }

  def agent = EdmAgent(
    name = Some("PA Digital"),
    uri = Some(new URI("http://dp.la/api/contributor/pa"))
  )

  // Get the last occurrence of the identifier property, there
  // must be at least three dc:identifier properties for there
  // to be a thumbnail
  def thumbnail(): ZeroToOne[EdmWebResource] = {
    val ids = extractStrings(xml \ "metadata" \\ "identifier")
    if (ids.size > 2)
      Option(uriOnlyWebResource(new URI(ids.last)))
    else
      None
  }

  def extractDataProvider(): ExactlyOne[EdmAgent] = {
    val contributors = extractStrings(xml \ "metadata" \\ "contributor")
    if (contributors.nonEmpty)
      nameOnlyAgent(contributors.last)
    else
      throw new Exception(s"Missing required property dataProvider because " +
        s"dc:contributor is empty for ${getProviderId()}")
  }

  def isUrl(url: String): Boolean = Try {new URL(url) }.isSuccess
}
