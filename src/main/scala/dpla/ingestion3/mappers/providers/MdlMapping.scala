package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

// FIXME Why is the implicit conversion not working for JValue when it is for NodeSeq?
class MdlMapping extends JsonMapping with JsonExtractor with IdMinter[JValue] with IngestMessageTemplates {

  val formatBlockList: Set[String] = ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "minnesota"

  override def getProviderId(implicit data: Document[JValue]): String =
    extractString(unwrap(data) \ "record" \ "isShownAt")
       .getOrElse(throw new RuntimeException(s"No ID for record: ${compact(data)}"))

  // OreAggregation
  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \\ "record" \ "dataProvider").map(nameOnlyAgent)

  override def dplaUri(data: Document[JValue]): ExactlyOne[URI] =
    new URI(mintDplaId(data))

  override def edmRights(data: Document[json4s.JValue]): ZeroToMany[URI] =
    extractStrings(unwrap(data)  \\ "record" \ "rights").map(URI)

  override def intermediateProvider(data: Document[JValue]): ZeroToOne[EdmAgent] =
    extractString(unwrap(data) \ "record" \ "intermediateProvider").map(nameOnlyAgent)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \\ "record" \ "isShownAt").map(stringOnlyWebResource)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \\ "record" \ "object").map(stringOnlyWebResource)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    extractCollection(unwrap(data) \ "record" \ "sourceResource" \ "collection")

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \\ "record" \ "sourceResource" \ "contributor").map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data)  \\ "record" \ "sourceResource" \ "creator").map(nameOnlyAgent)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractDate(unwrap(data)  \\ "record" \ "sourceResource" \ "date")

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \\ "record" \ "sourceResource" \ "description")

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data)  \\ "record" \\ "extent")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data)  \\ "record" \ "sourceResource" \ "format").map(_.applyBlockFilter(formatBlockList))

  override def genre(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data)  \\ "record" \ "sourceResource" \ "type").map(nameOnlyConcept)

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] = {
    val codes = extractStrings(unwrap(data) \\ "record" \ "sourceResource" \ "language" \ "iso636_3").map(nameOnlyConcept)
    val names = extractStrings(unwrap(data) \\ "record" \ "sourceResource" \ "language" \ "name").map(nameOnlyConcept)

    if (codes.nonEmpty)
      codes
    else
      names
  }

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    place(unwrap(data) \\ "record" \\ "sourceResource" \ "spatial")

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data)  \\ "record" \\ "sourceResource" \ "publisher").map(nameOnlyAgent)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data)  \\ "record" \\ "sourceResource" \ "rights")

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data)  \\ "record" \\ "sourceResource" \ "subject" \ "name").map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data)  \\ "record" \ "sourceResource" \ "title")

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data)  \\ "record" \ "sourceResource" \ "type")

  // Helper functions
  def extractCollection(collection: JValue): ZeroToMany[DcmiTypeCollection] = {
    iterify(collection).children.map(c => {
      DcmiTypeCollection(
        // TODO confirm this mapping
        // Example http://hub-client.lib.umn.edu/api/v1/records?q=32dc110dae8fa8471178e5fbfe5546446dcef7ec
        title = extractString(c \\ "title"),
        description = extractString(c \\ "description")
      )})
  }

  def extractDate(date: JValue): ZeroToMany[EdmTimeSpan] = {
    iterify(date).children.map(d =>
      EdmTimeSpan(
        begin = extractString(d \ "begin"),
        end = extractString(d \ "end"),
        originalSourceDate = extractString(d \ "displayDate")
      ))
  }

  def place(place: JValue): ZeroToMany[DplaPlace] = {
    iterify(place).children.map(p =>
      DplaPlace(
        // head is used b/c name and coordinate values can be string or arrays in original data
        // TODO Is there a cleaner way to handle this case in data ExtractionUtils extractString method?
        // See http://cqa-pa.internal.dp.la:8080/qa/compare?id=cfa88467c38a5fa871252c7e0a76962d
        // vs http://cqa-pa.internal.dp.la:8080/v2/items/8905a0065622a217b0d929cef3f62246?api_key=
        name = extractStrings(p \\ "name").headOption,
        coordinates = extractStrings(p \\ "coordinates").headOption
      ))
  }

  def agent = EdmAgent(
    name = Some("Minnesota Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/mdl"))
  )
}
