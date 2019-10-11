package dpla.ingestion3.mappers.utils

import scala.util.{Success, Try}
import scala.xml.{Node, NodeSeq}

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

  val leaderFormats: Map[Char, String] = Map(
    'a' -> "Language material",
    'c' -> "Notated music",
    'd' -> "Manuscript",
    'e' -> "Cartographic material",
    'f' -> "Manuscript cartographic material",
    'g' -> "Projected medium",
    'i' -> "Nonmusical sound recording"
  )

  val controlFormats: Map[Char, String] = Map(
    'a' -> "Map",
    'c' -> "Electronic resource",
    'd' -> "Globe",
    'f' -> "Tactile material",
    'g' -> "Projected graphic",
    'h' -> "Microform",
    'k' -> "Nonprojected graphic",
    'm' -> "Motion picture",
    'o' -> "Kit",
    'q' -> "Notated music",
    'r' -> "Remote-sensing image",
    's' -> "Sound recording",
    't' -> "Text",
    'v' -> "Videorecording",
    'z' -> "Unspecified"
  )

  /**
    * Extract a subject string from a Node
    *
    * @param node Node containing subject data
    * @return     String
    */
  def extractMarcSubject(node: Node): String = {
    val tag: String = node \@ "tag" // get tag for this datafield

    (node \ "subfield")
      .filter(n => ('a' to 'z').toList.map(_.toString).contains(n \@ "code")) // reject subfields with numeric codes
      .flatMap(subfield => {  // iterate through subfields

      val code: String = subfield \@ "code"

      // choose appropriate delimiter based on tag and/or code values
      val delimiter =
        if (tag == "658")
          code match {
            case "b" => ":"
            case "c" => ", "
            case "d" => "--"
            case _ => ". "
          }
        else if (tag == "653") "--"
        else if ((690 to 699).map(_.toString).contains(tag)) "--"
        else if (Seq("654", "655").contains(tag) && code == "b") "--"
        else if (Seq("v", "x", "y", "z").contains(code)) "--"
        else if (code == "d") ", "
        else ". "

      // strip trailing punctuation
      val text: String =
        if (delimiter == ".") subfield.text.stripSuffix(",").stripSuffix(".")
        else subfield.text.stripSuffix(",")

      // return delimiter and text - note that the delimiter goes before the text
      Seq(delimiter, text)

    }).drop(1).mkString("").stripSuffix(".") // drop leading delimiter and join substrings
  }
}