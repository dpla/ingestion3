package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToOne}
import dpla.ingestion3.model._

import scala.util.{Failure, Success, Try}
import scala.xml._

class PaExtractor(rawData: String) extends Extractor with XmlExtractionUtils {

  implicit val xml: NodeSeq = XML.loadString(rawData)

  def agent = EdmAgent(
    name = Some("Pennsylvania Digital Collections Project"),
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

  def dataProvider(): ExactlyOne[EdmAgent] = {
    val contributors = extractStrings(xml \ "metadata" \\ "contributor")
    if (contributors.nonEmpty)
      nameOnlyAgent(contributors.last)
    else
      throw new Exception("Missing required property dataProvider because dc:contributor is empty")
  }
  // Get the second occurrence of the dc:identifier property
  def itemUri(): ExactlyOne[URI] = {
    val ids = extractStrings(xml \ "metadata" \\ "identifier")
    if (ids.size >= 2)
      new URI(ids(1))
    else
    // TODO Are these exception messages valid? Or is there a cleaner way of writing these?
      throw new Exception("Missing required property itemUri because dc:identifier " +
        s"does not occur at least twice in record: ${getProviderBaseId().getOrElse("No ID available!")}")
  }

  def build: DplaMap = Try {
    DplaMapData(
      DplaSourceResource(
        // This method of using NodeSeq is required because of namespace issues.
        collection = extractStrings(xml \ "metadata" \\ "relation").headOption.map(nameOnlyCollection).toSeq,
        contributor = extractStrings(xml \ "metadata" \\ "contributor").dropRight(1).map(nameOnlyAgent),
        creator = extractStrings(xml \ "metadata" \\ "creator").map(nameOnlyAgent),
        date = extractStrings(xml \ "metadata" \\ "date").map(stringOnlyTimeSpan),
        description = extractStrings(xml \ "metadata" \\ "description"),
        format = extractStrings(xml \ "metadata" \\ "type").filterNot(isDcmiType),
        genre = extractStrings(xml \ "metadata" \\ "type").map(nameOnlyConcept),
        identifier = extractStrings(xml \ "metadata" \\ "identifier"),
        language = extractStrings(xml \ "metadata" \\ "language").map(nameOnlyConcept),
        place = extractStrings(xml \ "metadata" \\ "coverage").map(nameOnlyPlace),
        publisher = extractStrings(xml \ "metadata" \\ "publisher").map(nameOnlyAgent),
        relation = extractStrings(xml \ "metadata" \\ "relation").drop(1).map(eitherStringOrUri),
        rights = extractStrings(xml \ "metadata" \\ "rights"),
        subject = extractStrings(xml \ "metadata" \\ "subject").map(nameOnlyConcept),
        title = extractStrings(xml \ "metadata" \\ "title"),
        `type` = extractStrings(xml \ "metadata" \\ "type").filter(isDcmiType)
      ),

      EdmWebResource(
        // TODO is this the correct mapping for uri? What about OreAgg.`object`?
        uri = itemUri,
        fileFormat = extractStrings("dc:format")
      ),

      OreAggregation(
        uri = mintDplaItemUri(),
        //below will throw if not enough contributors
        dataProvider = dataProvider(),
        originalRecord = rawData,
        provider = agent,
        preview = thumbnail
        // TODO what about `object` in this context? Assume no implementation
      )
    )
  }

  override def getProviderBaseId(): Option[String] = extractString(xml \ "header" \ "identifier")
}
