package dpla.ingestion3.harvesters.oai

import scala.xml.{Node, XML}

object OaiXmlParser {

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
