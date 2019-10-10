package dpla.ingestion3.mappers.utils

import scala.util.{Success, Try}
import scala.xml.NodeSeq

trait MarcXmlMapping extends XmlMapping with XmlExtractor {
  /**
    * Get <dataset><subfield> nodes by tag and code
    *
    * @param data   Document
    * @param tags   Seq[String] tags for <dataset>
    * @param codes  Seq[String] codes for <subfield> (if empty or undefined, all <subfield> nodes will be returned)
    * @return       Seq[NodeSeq] <subfield> nodes
    */
  def marcFields(data: Document[NodeSeq], tags: Seq[String], codes: Seq[String] = Seq()): Seq[NodeSeq] = {
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
  def datafield(data: Document[NodeSeq], tags: Seq[String]): NodeSeq =
    (data \ "datafield").flatMap(n => getByAttributeListOptions(n, "tag", tags))

  /**
    * Filter <subfield> nodes by code
    *
    * @param subfields  NodeSeq <subfield> nodes
    * @param codes      Seq[String] codes for <subfield>
    * @return           NodeSeq <subfield> nodes
    */
  def filterSubfields(subfields: NodeSeq, codes: Seq[String]): NodeSeq =
    subfields.flatMap(n => getByAttributeListOptions(n, "code", codes))

  /**
    * Get <controlfield> nodes by code
    *
    * @param data   Document
    * @param tags   Seq[String] codes for <controlfield>
    * @return       NodeSeq <controlfield> nodes
    */
  def controlfield(data: Document[NodeSeq], tags: Seq[String]): NodeSeq =
    (data \ "controlfield").flatMap(n => getByAttributeListOptions(n, "tag", tags))

  /**
    * Get the character at a specified index of a <controlfield> node
    *
    * @param data   Document
    * @param tag    String tag for <controlfield> node
    * @param index  Int index of the desired character
    * @return       Option[Char] character if found
    */
  def controlAt(data: Document[NodeSeq], tag: String, index: Int): Seq[Char] =
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
  def leader(data: Document[NodeSeq]): String =
    extractStrings(data \ "leader").headOption.getOrElse("")

  /**
    * Get the character at a specified index of the <leader> text
    *
    * @param data   Document
    * @param index  Int index of the desired character
    * @return       Option[Char] character if found
    */
  def leaderAt(data: Document[NodeSeq], index: Int): Option[Char] = {
    Try {
      leader(data).charAt(index)
    }.toOption
  }
}