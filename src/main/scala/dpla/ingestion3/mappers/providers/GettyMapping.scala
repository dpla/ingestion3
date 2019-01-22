package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.util.Try
import scala.xml._


class GettyMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {

  val extentAllowList: Set[String] =
    ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "getty"

  override def getProviderId(implicit data: Document[NodeSeq]): String =
    extractString(data \\ "PrimoNMBib" \ "record" \ "control" \ "recordid")
      .getOrElse(throw new RuntimeException(s"No ID for record $data"))

  // SourceResource mapping
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    // display/lds43 AND display/lds34
    (extractStrings(data \\ "display" \ "lds43") ++
      extractStrings(data \\ "display" \ "lds34"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \\ "display" \ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    (extractStrings(data \\ "display" \ "creator") ++
      extractStrings(data \\ "display" \ "lds50"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    // display/creationdate
      extractStrings(data \\ "display" \ "creationdate")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
  // display/lds04 AND display/lds28 AND display/format
    extractStrings(data \\ "display" \ "lds04") ++
      extractStrings(data \\ "display" \ "lds28") ++
      extractStrings(data \\ "display" \ "format")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \\ "display" \ "lds09")
      .map(_.applyBlockFilter(extentAllowList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "display" \ "lds14")
      .flatMap(_.splitAtDelimiter(";"))

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "display" \ "language")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \\ "display" \ "coverage")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \\ "display" \ "publisher")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    // display/lds27 AND display/rights
    ((data \\ "display" \ "rights") ++
      (data \\ "display" \ "lds27"))
      .flatMap(extractStrings)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
  // display/subject AND display/lds49
    (extractStrings(data \\ "display" \ "subject") ++
      extractStrings(data \\ "display" \ "lds49"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    // display/title AND display/lds03
    (extractStrings(data \\ "display" \ "title") ++
      extractStrings(data \\ "display" \ "lds03"))
      .flatMap(_.splitAtDelimiter(";"))

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    // display/lds26
    extractStrings(data \\ "display" \ "lds26")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def originalId(data: Document[NodeSeq]): ZeroToOne[String] = Try{ Some(getProviderId(data)) }.getOrElse(None)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    Seq(nameOnlyAgent("Getty Research Institute"))

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val baseIsShownAt =
      "http://primo.getty.edu/primo_library/libweb/action/dlDisplay.do?vid=GRI-OCP&afterPDS=true&institution=01GRI&docId="

    extractString(data \\"control" \ "sourceid") match {
      case Some("GETTY_ROSETTA") =>
        extractStrings(data \\ "display" \ "lds29")
          .map(stringOnlyWebResource)
      case Some("GETTY_OCP") =>
        extractStrings(data \\ "control" \ "recordid")
          .map(baseIsShownAt + _.trim)
          .map(stringOnlyWebResource)
      case _ => Seq()
    }
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // sear/thumbnail
    extractStrings(data \\ "LINKS" \ "thumbnail")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("J. Paul Getty Trust"),
    uri = Some(URI("http://dp.la/api/contributor/getty"))
  )
}

