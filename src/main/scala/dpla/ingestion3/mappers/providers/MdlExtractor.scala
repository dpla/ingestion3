//package dpla.ingestion3.mappers.providers
//
//import java.net.URI
//
//import dpla.ingestion3.mappers.json.JsonExtractionUtils
//import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany}
//import dpla.ingestion3.model.{DplaSourceResource, EdmWebResource, OreAggregation, _}
//import org.json4s.JsonDSL._
//import org.json4s._
//import org.json4s.jackson.JsonMethods._
//
//import scala.util.Try
//
//class MdlExtractor(rawData: String, shortName: String) extends Extractor with JsonExtractionUtils {
//  implicit val json: JValue = parse(rawData)
//
//  // ID minting functions
//  override def useProviderName(): Boolean = true
//
//  override def getProviderName(): String = "minnesota"
//
//  override def getProviderId(): String = extractString(json \ "record" \ "isShownAt")
//    .getOrElse(throw ExtractorException(s"No ID for record: ${compact(json)}"))
//
//  def build: Try[OreAggregation] = {
//    Try {
//      OreAggregation(
//        dplaUri = new URI(mintDplaId()),
//        sidecar = ("prehashId", buildProviderBaseId()) ~
//                  ("dplaId", mintDplaId()),
//        sourceResource = DplaSourceResource(
//          // There is inconsistent quality between originalRecord.collection and sourceResource.collection
//          // It is not clear why but someimtes OR has title and desc but SR only has title and vice versa
//          collection = collection(json \ "record" \ "sourceResource" \ "collection"), // TODO confirm this mapping
//          contributor = extractStrings(json \\ "record" \ "sourceResource" \ "contributor").map(nameOnlyAgent),
//          creator = extractStrings(json \\ "record" \ "sourceResource" \ "creator").map(nameOnlyAgent),
//          date = date(json \\ "record" \ "sourceResource" \ "date"),
//          description = extractStrings(json \\ "record" \ "sourceResource" \ "description"),
//          extent = extractStrings(json \\ "record" \\ "extent"),
//          format = extractStrings(json \\ "record" \ "sourceResource" \ "format"),
//          genre = extractStrings(json \\ "record" \ "sourceResource" \ "type").map(nameOnlyConcept),
//          language = extractStrings(json \\ "record" \ "sourceResource" \ "language" \ "iso636_3").map(nameOnlyConcept)
//            ++ extractStrings(json \\ "record" \ "sourceResource" \ "language" \ "name").map(nameOnlyConcept),
//          place = place(json \\ "record" \ "sourceResource" \ "spatial"),
//          publisher = extractStrings(json \\ "record" \ "sourceResource" \ "publisher").map(nameOnlyAgent),
//          rights = extractStrings(json \\ "record" \ "sourceResource" \ "rights"),
//          subject = extractStrings(json \\ "record" \ "sourceResource" \ "subject" \ "name").map(nameOnlyConcept),
//          title = extractStrings(json \\ "record" \ "sourceResource" \ "title"),
//          `type` = extractType // FIXME
//        ),
//        dataProvider = dataProvider(json \\ "record" \ "dataProvider"),
//        originalRecord = rawData,
//        isShownAt = uriOnlyWebResource(uri(json \\ "record" \ "isShownAt")), // FIXME see ingest1 code
//        preview = thumbnail(json \\ "record" \ "object"),
//        provider = agent
//      )
//    }
//  }
//
//  def collection(collection: JValue): ZeroToMany[DcmiTypeCollection] = {
//    iterify(collection).children.map(c => {
//      DcmiTypeCollection(
//        // TODO confirm this mapping
//        // Example http://hub-client.lib.umn.edu/api/v1/records?q=32dc110dae8fa8471178e5fbfe5546446dcef7ec
//        title = extractString(c \\ "title"),
//        description = extractString(c \\ "description")
//      )})
//  }
//
//  def dataProvider(dataProvider: JValue): ExactlyOne[EdmAgent] = {
//    // Data providers come in arrays and as single values here so we extract
//    // as a Seq and take the first one
//    extractStrings(dataProvider)
//      .headOption
//      .map(nameOnlyAgent)
//      .getOrElse(throw new RuntimeException(s"dataProvider is missing for\t" +
//        s"${
//          extractString(json \\ "isShownAt")
//            .getOrElse(pretty(render(json)))
//        }"))
//  }
//
//  def date(date: JValue): ZeroToMany[EdmTimeSpan] = {
//    iterify(date).children.map(d =>
//      EdmTimeSpan(
//        begin = extractString(d \ "begin"),
//        end = extractString(d \ "end"),
//        originalSourceDate = extractString(d \ "displayDate")
//      ))
//  }
//
//  def extractType(): ZeroToMany[String] = {
////    if not getprop(self.mapped_data, "sourceResource/type", True):
////      object_id = getprop(self.provider_data, "sourceResource/object", True)
////    if object_id:
////      self.update_source_resource({"type": "image"}
//    extractStrings(json \\ "record" \ "sourceResource" \ "type") // FIXME see DCMI type enrichment
//  }
//
//  def place(place: JValue): ZeroToMany[DplaPlace] = {
//    iterify(place).children.map(p =>
//      DplaPlace(
//        // head is used b/c name and coordinate values can be string or arrays in original data
//        // TODO Is there a cleaner way to handle this case in JsonExtractionUtils extractString method?
//        // See http://cqa-pa.internal.dp.la:8080/qa/compare?id=cfa88467c38a5fa871252c7e0a76962d
//        // vs http://cqa-pa.internal.dp.la:8080/v2/items/8905a0065622a217b0d929cef3f62246?api_key=
//        name = extractStrings(p \\ "name").headOption,
//        coordinates = extractStrings(p \\ "coordinates").headOption
//      ))
//  }
//
//  def thumbnail(thumbnail: JValue): Option[EdmWebResource] =
//    extractString(thumbnail) match {
//      case Some(t) => Some(
//        uriOnlyWebResource(new URI(t))
//      )
//      case None => None
//    }
//
//  def agent = EdmAgent(
//    name = Some("Minnesota Digital Library"),
//    uri = Some(new URI("http://dp.la/api/contributor/mdl"))
//  )
//
//  def uri(uri: JValue): URI = {
//    extractString(uri) match {
//      case Some(t) => new URI(t)
//      case _ => throw new RuntimeException(s"isShownAt is missing. Cannot map record.")
//    }
//  }
//}
