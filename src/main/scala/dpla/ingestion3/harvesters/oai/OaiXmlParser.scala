package dpla.ingestion3.harvesters.oai

import scala.xml.{Node, XML}

object OaiXmlParser {

  /** Regex to extract OAI record identifiers from raw page text (best-effort). */
  private val IdentifierPattern = """<identifier>([^<]+)</identifier>""".r

  def parsePageIntoXml(
      page: OaiPage
  ): Node = {
    // some hubs have inline xml processing instructions that blow things up
    val cleaned = page.page.replaceAll(raw"<\?xml[^>]*\?>", "")
    XML.loadString(cleaned)
  }

  def parseXmlIntoRecords(
      xml: Node,
      removeDeleted: Boolean,
      info: OaiRequestInfo
  ): Seq[OaiRecord] = {
    containsError(xml)
    getRecords(xml, removeDeleted, info)
  }

  def parseXmlIntoSets(
      xml: Node,
      info: OaiRequestInfo
  ): Seq[OaiSet] = {
    containsError(xml)
    getSets(xml, info)
  }

  def getResumptionToken(xml: Node): Option[String] = {
    val resumptionToken = (xml \\ "resumptionToken").text
    if (resumptionToken.nonEmpty) Some(resumptionToken) else None
  }

  /** Extract the resumption token value along with optional cursor and
    * completeListSize attributes from the parsed XML.
    *
    * @return Some((tokenValue, cursor, completeListSize)) or None
    */
  def getResumptionTokenWithAttrs(xml: Node): Option[(String, Option[Int], Option[Int])] = {
    val tokenNodes = xml \\ "resumptionToken"
    val tokenText = tokenNodes.text
    if (tokenText.isEmpty) return None

    val cursor = tokenNodes.headOption
      .flatMap(n => Option(n \@ "cursor"))
      .filter(_.nonEmpty)
      .flatMap(s => scala.util.Try(s.toInt).toOption)

    val completeListSize = tokenNodes.headOption
      .flatMap(n => Option(n \@ "completeListSize"))
      .filter(_.nonEmpty)
      .flatMap(s => scala.util.Try(s.toInt).toOption)

    Some((tokenText, cursor, completeListSize))
  }

  /** Best-effort extraction of first and last record identifiers from raw page
    * text using regex. Works even when the XML is malformed.
    *
    * @return (firstId, lastId) — either may be None
    */
  def extractIdentifiers(rawPage: String): (Option[String], Option[String]) = {
    val ids = IdentifierPattern.findAllMatchIn(rawPage).map(_.group(1)).toSeq
    (ids.headOption, ids.lastOption)
  }

  private def getRecords(xml: Node, removeDeleted: Boolean, info: OaiRequestInfo): Seq[OaiRecord] =
    for {
      record <- xml \ "ListRecords" \ "record"
      id = (record \ "header" \ "identifier").text
      status = (record \ "header" \ "@status").headOption
        .getOrElse(<foo>foo</foo>)
        .text
      if !removeDeleted || status != "deleted"
    } yield OaiRecord(id, record.toString, info)

  private def getSets(xml: Node, info: OaiRequestInfo): Seq[OaiSet] =
    for (set <- xml \ "ListSets" \ "set")
      yield {
        val id = (set \ "setSpec").text
        OaiSet(id, set.toString, info)
      }

  def containsError(xml: Node): Unit = {
    val error = xml \ "error"
    if (error.nonEmpty && error \@ "code" != "noRecordsMatch"  )
      throw new RuntimeException("Error in OAI response: " + error.text)
  }
}
