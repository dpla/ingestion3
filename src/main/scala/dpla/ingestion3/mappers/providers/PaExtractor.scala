package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model.DplaMapData.ZeroToOne
import dpla.ingestion3.model._

import scala.xml._

class PaExtractor(rawData: String) extends Extractor with XmlExtractionUtils {

  implicit val xml: NodeSeq = XML.load(rawData)

  def agent = EdmAgent(
    name = Some("Pennsylvania Digital Collections Project"),
    uri = Some(new URI("http://dp.la/api/contributor/pa"))
  )

  // Get the last occurrence of the identifier property
  def thumbnail(): ZeroToOne[EdmWebResource] = {
    val ids = extractStrings("dc:identifier")
    if (ids.size > 2)
      Option(uriOnlyWebResource(new URI(ids.last)))
    else
      None
  }

  def build: DplaMapData = {
    lazy val itemUrl = new URI(extractStrings("dc:identifier").apply(1))

    DplaMapData(
      DplaSourceResource(
        collection = extractStrings("dc:relation").headOption.map(nameOnlyCollection).toSeq,
        contributor = extractStrings("dc:contributor").dropRight(1).map(nameOnlyAgent),
        creator = extractStrings("dc:creator").map(nameOnlyAgent),
        date = extractStrings("dc:date").map(stringOnlyTimeSpan),
        description = extractStrings("dc:description"),
        format = extractStrings("dc:type").filterNot(isDcmiType),
        genre = extractStrings("dc:type").map(nameOnlyConcept),
        identifier = extractStrings("dc:identifier"),
        language = extractStrings("dc:language").map(nameOnlyConcept),
        place = extractStrings("dc:coverage").map(nameOnlyPlace),
        publisher = extractStrings("dc:publisher").map(nameOnlyAgent),
        relation = extractStrings("dc:relation").drop(1).map(eitherStringOrUri),
        rights = extractStrings("dc:rights"),
        subject = extractStrings("dc:subject").map(nameOnlyConcept),
        title = extractStrings("dc:title"),
        `type` = extractStrings("dc:type").filter(isDcmiType)
      ),

      EdmWebResource(
        // TODO is this the correct mapping for uri? What about OreAgg.`object`?
        uri = itemUrl,
        fileFormat = extractStrings("dc:format")
      ),

      OreAggregation(
        uri = mintDplaItemUri(),
        //below will throw if not enough contributors
        dataProvider = nameOnlyAgent(extractStrings("dc:contributor").last),
        originalRecord = rawData,
        provider = agent,
        preview = thumbnail
        // TODO what about `object` in this context? Assume no implementation
      )
    )
  }

  override def getProviderBaseId(): Option[String] = extractString("id")(xml)
}
