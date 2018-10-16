package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class MwdlMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {

  private val baseIsShownAt = "http://utah-primoprod.hosted.exlibrisgroup.com/primo_library/libweb/action/dlDisplay.do?vid=MWDL&afterPDS=true&docId="
  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "mwdl"

  override def getProviderId(implicit data: Document[NodeSeq]): String =
    extractString(data \\ "PrimoNMBib" \ "record" \ "control" \ "recordid")
      .getOrElse(throw new RuntimeException(s"No ID for record $data"))

  // SourceResource mapping
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(data \\ "search" \ "lsr13")
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \\ "display" \ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \\ "display" \ "creator")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    // search/creationdate AND PrimoNMBib/record/display/creationdate
    (extractStrings(data \\ "search" \\ "creationdate") ++
      extractStrings(data \\ "display" \ "creationdate"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
  // search/description (contains dc:description, dcterms:abstract, and dcterms:tableOfContents)
    extractStrings(data \\ "search" \ "description")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \\ "display" \ "lds05")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "control" \ "recordid")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "facets" \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \\ "display" \ "lds08")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \\ "display" \ "relation")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \\ "display" \ "relation")
      .flatMap(_.splitAtDelimiter(";"))
      .map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    (data \\ "display" \ "rights")
      .flatMap(extractStrings)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
  // display/subject
    extractStrings(data \\ "display" \ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \\ "display" \ "lds09")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "display" \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
  // facets/rsrctype
    extractStrings(data \\ "facets" \ "rsrctype")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (data \\ "display" \ "lds03")
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // baseIsShownAt + control\recordid
    (data \\ "control" \ "recordid")
      .flatMap(extractStrings)
      .map(baseIsShownAt + _)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (data \\ "LINKS" \ "thumbnail")
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Mountain West Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/mwdl"))
  )
}

