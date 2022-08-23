package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._


class GettyMapping extends JsonMapping with JsonExtractor {

  val extentAllowList: Set[String] =
    ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): Option[String] = Some("getty")

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] = {
    extractStrings(unwrap(data) \\ "control" \ "recordid").headOption
  }

  // SourceResource mapping
  override def collection(data: Document[JValue]): Seq[DcmiTypeCollection] =
  // display/lds43 AND display/lds34
    (extractStrings(unwrap(data) \\ "display" \ "lds43") ++
      extractStrings(unwrap(data) \\ "display" \ "lds34"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[JValue]): Seq[EdmAgent] =
    extractStrings(unwrap(data) \\ "display" \ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def creator(data: Document[JValue]): Seq[EdmAgent] =
    (extractStrings(unwrap(data) \\ "display" \ "creator") ++
      extractStrings(unwrap(data) \\ "display" \ "lds50"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[JValue]): Seq[EdmTimeSpan] =
  // display/creationdate
    extractStrings(unwrap(data) \\ "display" \ "creationdate")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[JValue]): Seq[String] =
  // display/lds04 AND display/lds28 AND display/format
    extractStrings(unwrap(data) \\ "display" \ "lds04") ++
      extractStrings(unwrap(data) \\ "display" \ "lds28") ++
      extractStrings(unwrap(data) \\ "display" \ "format")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \\ "display" \ "lds09")
      .map(_.applyBlockFilter(extentAllowList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \\ "display" \ "lds14")
      .flatMap(_.splitAtDelimiter(";"))

  override def language(data: Document[JValue]): Seq[SkosConcept] =
    extractStrings(unwrap(data) \\ "display" \ "language")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): Seq[DplaPlace] =
    extractStrings(unwrap(data) \\ "display" \ "coverage")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyPlace)

  override def publisher(data: Document[JValue]): Seq[EdmAgent] =
    extractStrings(unwrap(data) \\ "display" \ "publisher")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
  // display/lds27 AND display/rights
    extractStrings((unwrap(data) \\ "display" \ "rights") ++
      extractStrings(unwrap(data) \\ "display" \ "lds27"))

  override def subject(data: Document[JValue]): Seq[SkosConcept] =
  // display/subject AND display/lds49
    (extractStrings(unwrap(data) \\ "display" \ "subject") ++
      extractStrings(unwrap(data) \\ "display" \ "lds49"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def title(data: Document[JValue]): Seq[String] =
  // display/title AND display/lds03
    (extractStrings(unwrap(data) \\ "display" \ "title") ++
      extractStrings(unwrap(data) \\ "display" \ "lds03"))
      .flatMap(_.splitAtDelimiter(";"))

  override def `type`(data: Document[JValue]): Seq[String] =
  // display/lds26
    extractStrings(unwrap(data) \\ "display" \ "lds26")

  // OreAggregation
  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    Seq(nameOnlyAgent("Getty Research Institute"))

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] = {
//    val baseIsShownAt = "https://primo.getty.edu/primo-explore/fulldisplay?vid=GRI-OCP&context=L&tab=all_gri&lang=en_US&docid="
//
//    extractString(unwrap(data) \\ "control" \ "sourceid") match {
//      case Some("GETTY_ROSETTA") =>
        extractStrings(unwrap(data) \ "delivery" \ "availabilityLinksUrl")
          .map(stringOnlyWebResource)
//      case Some("GETTY_OCP") =>
//        extractStrings(unwrap(data) \\ "control" \ "recordid")
//          .map(baseIsShownAt + _.trim)
//          .map(stringOnlyWebResource)
//      case _ => Seq()
//    }
  }

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] = Utils.formatJson(unwrap(data))

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] = {
  // sear/thumbnail
//    "link": [
  //          {
  //            "displayLabel": "thumbnail",
  //            "hyperlinkText": "",
  //            "inst4opac": "01GRI",
  //            "linkURL": "https://rosettaapp.getty.edu/delivery/DeliveryManagerServlet?dps_pid=IE1318448&dps_func=thumbnail",
  //            "linkType": "http://purl.org/pnx/linkType/thumbnail",
  //            "@id": "_:0"
  //          },
  (unwrap(data) \ "delivery" \ "link")
    .filter(extractString("displayLabel")(_).getOrElse("").equalsIgnoreCase("thumbnail"))
    .flatMap(extractStrings("linkURL")(_))
    .map(stringOnlyWebResource)
  }

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("J. Paul Getty Trust"),
    uri = Some(URI("http://dp.la/api/contributor/getty"))
  )
}

