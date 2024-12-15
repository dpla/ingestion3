package dpla.ingestion3.harvesters.oai.refactor

import scala.xml.{Node, XML}

object OaiXmlParser {

  def parsePageIntoXml(
      page: OaiPage
  ): Node = XML.loadString(page.page)

  def parseXmlIntoRecords(
      xml: Node,
      removeDeleted: Boolean
  ): Seq[OaiRecord] = {
    containsError(xml)
    getRecords(xml, removeDeleted)
  }

  def parseXmlIntoSets(
      xml: Node
  ): Seq[OaiSet] = {
    containsError(xml)
    getSets(xml)
  }

  def getResumptionToken(xml: Node): Option[String] = {
    val resumptionToken = (xml \\ "resumptionToken").text
    if (resumptionToken.nonEmpty) Some(resumptionToken) else None
  }

  private def getRecords(xml: Node, removeDeleted: Boolean): Seq[OaiRecord] =
    for {
      record <- xml \ "ListRecords" \ "record"
      id = (record \ "header" \ "identifier").text
      setIds = for (set <- record \ "header" \ "setSpec") yield set.text
      status = (record \ "header" \ "@status").headOption
        .getOrElse(<foo>foo</foo>)
        .text
      if !removeDeleted || status != "deleted"
    } yield OaiRecord(id, record.toString, setIds)

  private def getSets(xml: Node): Seq[OaiSet] =
    for (set <- xml \ "ListSets" \ "set")
      yield {
        val id = (set \ "setSpec").text
        OaiSet(id, set.toString)
      }

  def containsError(xml: Node): Unit = {
    val error = xml \ "error"
    if (error.nonEmpty && error \@ "code" != "noRecordsMatch"  )
      throw new RuntimeException("Error in OAI response: " + error.text)
  }
}
