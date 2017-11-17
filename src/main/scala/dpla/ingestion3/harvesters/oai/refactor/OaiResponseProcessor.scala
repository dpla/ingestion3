package dpla.ingestion3.harvesters.oai.refactor

import scala.util.{Failure, Success, Try}
import scala.xml.{Node, NodeSeq, XML}

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  */
object OaiResponseProcessor {

  /**
    * Get the resumptionToken from the response
    *
    * @param page String
    *             The String response from an OAI request
    * @return Option[String]
    *         The resumptionToken to fetch the next set of records
    *         or None if no more records can be fetched. An
    *         empty string does not mean all records were successfully
    *         harvested (an error could have occurred when fetching), only
    *         that there are no more records that can be fetched.
    */
  def getResumptionToken(page: String): Option[String] = {
    val pattern = """<resumptionToken.*>(.*)</resumptionToken>""".r
    pattern.findFirstMatchIn(page) match {
      case Some(m) => Some(m.group(1))
      case _ => None
    }
  }

  /**
    * Parses records from an OaiSource.
    *
    * @param page:  OaiPage
    *               The single-page response to a single OAI query.
    *
    * @return OaiResponse
    *         OaiRecordsPage - all the records appearing on the page OR
    *         OaiError - an error incurred during the process of parsing the records.
    */

  def getRecordsFromPage(page: OaiPage): OaiResponse = {
    getXml(page.page) match {
      case Failure(e) => OaiError(e.toString)
      case Success(xml) =>
        val records = getRecordsFromXml(xml)
        RecordsPage(records)
    }
  }

  /**
    * Try to parse XML from a given String, representing a valid OAI response.
    *
    * @param string String
    *               A string response from an OAI feed.
    *
    * @return Try[Node]
    *         An valid XML node, OR
    *         A failure if the XML is invalid or contains an OAI error message.
    */
  private def getXml(string: String): Try[Node] = Try {
    val xml = XML.loadString(string)
    getOaiErrorCode(xml)
    xml
  }

  /**
    * Parse an error message from an XML node.  Throw an excepetion if an error
    * is found.
    *
    * @param xml: NodeSeq
    *             The XML that may include an error message.
    * @return Unit
    * @throws RuntimeException
    */
  private def getOaiErrorCode(xml: NodeSeq): Unit = {
    if ((xml \ "error").nonEmpty)
      throw new RuntimeException((xml \ "error").text.trim)
  }

  // Parse all records from XML
  private def getRecordsFromXml(xml: Node): Seq[OaiRecord] = {
    val xmlRecords: NodeSeq = (xml \ "ListRecords" \ "record")

    xmlRecords.flatMap(record =>
      record.headOption match {
        case Some(node) =>
          val id = getRecordIdentifier(node)
          val setIds = getSetIdsFromRecord(node)
          Some(OaiRecord(id, node.toString, setIds ))
        case _ => None
      })
  }

  // TODO: To ensure continuity of IDs between ingestion systems and generalize the ID
  /**
    * Accepts a record from an OAI feed an returns the OAI identifier
    *
    * @param record The original record from the OAI feed
    * @return The local OAI identifier
    */
  private def getRecordIdentifier(record: Node): String =
    (record \ "header" \ "identifier").text

  /**
    * Get all set ids from a single record.
    * Return empty Seq if none exist.
    *
    * @return Seq[String]
    *         The set ids.
    */
  private def getSetIdsFromRecord(record: Node): Seq[String] =
    for (set <- record \ "header" \ "setSpec") yield set.text
}
