package dpla.ingestion3.mappers.providers

import java.net.URL

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, XmlExtractor, XmlMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.json4s.jackson.JsonMethods._
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scala.xml._


class GpoMapping extends XmlMapping with XmlExtractor {

  val isShownAtPrefix: String = ???

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "gpo"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] = ???

  // SourceResource mapping

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <datafield> tag = 700, 710, or 711
    marcFields(data, Seq("700", "710", "711"))
      .filterNot(filterSubfields(_, Seq("e")) // exclude if subfield with @code=e exists and...
        .flatMap(extractStrings)
        .exists(_ == "author") // ...#text = "author"
      )
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <datafield> tag = 100, 110, or 111
    marcFields(data, Seq("100", "110", "111"))
      .filter(filterSubfields(_, Seq("e")) // include if subfield with @code=e exists and...
        .flatMap(extractStrings)
        .exists(_ == "author") // ...#text = "author"
      )
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    // <datafield> tag = 260 or 264   <subfield> code = c
    // <datafield> tag = 262
    (marcFields(data, Seq("260", "264"), Seq("c")) ++ marcFields(data, Seq("262")))
      .flatMap(extractStrings)
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] = ???

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    // <datafield> tag = 300  <subfield> code = a
    marcFields(data, Seq("300"), Seq("a"))
      .flatMap(extractStrings)
      .map(_.stripSuffix(":"))

  override def format(data: Document[NodeSeq]): ZeroToMany[String] = ???

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    // <datafield> tag = 001, 020, or 022
    // <datafield> tag = 035, 050, 074, 082, or 086  <subfield> code = a
    (marcFields(data, Seq("001", "020", "022")) ++ marcFields(data, Seq("035", "050", "074", "082", "086"), Seq("a")))
      .flatMap(extractStrings)

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = ???

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = ???

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <datafield> tag = 260 or 264   <subfield> code = a or b
    marcFields(data, Seq("260", "264"), Seq("a, b"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] = ???

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] = ???

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = ???

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = ???

  override def title(data: Document[NodeSeq]): ZeroToMany[String] = ???

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] = ???

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = ???

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = ???

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = ???

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("United States Government Publishing Office (GPO)"),
    uri = Some(URI("http://dp.la/api/contributor/gpo"))
  )

  /**
    * Get <dataset><subfield> nodes by tag and code
    *
    * @param data   Document
    * @param tags   Seq[String] tags for <dataset>
    * @param codes  Seq[String] codes for <subfield> (if empty or undefined, all <subfield> nodes will be returned)
    * @return       Seq[NodeSeq] <subfield> nodes
    */
  private def marcFields(data: Document[NodeSeq], tags: Seq[String], codes: Seq[String] = Seq()): Seq[NodeSeq] = {
    val sub: Seq[NodeSeq] = datafield(data, tags).map(n => n \ "subfield")
    if (codes.nonEmpty) sub.map(n => filterSubfields(n, codes)) else sub
  }

  /**
    * Get <dataset> nodes by tag
    *
    * @param data   Document
    * @param tags   Seq[String] tags for <dataset>
    * @return       NodeSeq <dataset> nodes
    */
  private def datafield(data: Document[NodeSeq], tags: Seq[String]): NodeSeq =
    (data \ "datafield").flatMap(n => getByAttributeListOptions(n, "tag", tags))

  /**
    * Filter <subfield> nodes by code
    *
    * @param subfields  NodeSeq <subfield> nodes
    * @param codes      Seq[String] codes for <subfield>
    * @return           NodeSeq <subfield> nodes
    */
  private def filterSubfields(subfields: NodeSeq, codes: Seq[String]): NodeSeq =
    subfields.flatMap(n => getByAttributeListOptions(n, "code", codes))

  /**
    * Get <controlfield> nodes by code
    *
    * @param data   Document
    * @param tags   Seq[String] codes for <controlfield>
    * @return       NodeSeq <controlfield> nodes
    */
  private def controlfield(data: Document[NodeSeq], tags: Seq[String]): NodeSeq =
    (data \ "controlfield").flatMap(n => getByAttributeListOptions(n, "tag", tags))

  /**
    * Get the character at a specified index of a <controlfield> node
    *
    * @param data   Document
    * @param tag    String tag for <controlfield> node
    * @param index  Int index of the desired character
    * @return       Option[Char] character if found
    */
  private def controlAt(data: Document[NodeSeq], tag: String, index: Int): Seq[Char] =
    Try {
      controlfield(data, Seq(tag))
        .flatMap(extractStrings)
        .map(_.charAt(index))
    } match {
      case Success(c) => c
      case _ => Seq()
    }

  /**
    * Get <leader> node
    *
    * @param data   Document
    * @return       String text value of <leader> (empty String if leader not found)
    */
  private def leader(data: Document[NodeSeq]): String =
    extractStrings(data \ "leader").headOption.getOrElse("")

  /**
    * Get the character at a specified index of the <leader> text
    *
    * @param data   Document
    * @param index  Int index of the desired character
    * @return       Option[Char] character if found
    */
  private def leaderAt(data: Document[NodeSeq], index: Int): Option[Char] = {
    Try {
      leader(data).charAt(index)
    }.toOption
  }
}
