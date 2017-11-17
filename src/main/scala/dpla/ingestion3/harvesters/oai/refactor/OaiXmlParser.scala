package dpla.ingestion3.harvesters.oai.refactor

import scala.util.{Failure, Success, Try}
import scala.xml.{Node, NodeSeq, XML}

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  */
object OaiXmlParser {

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
    * Parse an OaiPage into an XML Node.
    * Return an OaiError if the XML is invalid or if it contains an error message.
    *
    * @param pageEither
    * @return
    */
  def parsePageIntoXml(pageEither: Either[OaiError, OaiPage]):
    Either[OaiError, Node] = {

    pageEither match {
      case Left(error) => Left(error)
      case Right(page) =>
        parseStringIntoXml(page.page) match {
          case Failure(e) => Left(OaiError(e.toString))
          case Success(xml) => Right(xml)
        }
    }
  }

  /**
    * Parses records from an OaiSource.
    *
    * @param xmlEither:  Either[OaiError, Node]
    *                    Node - an XML node containing 0-n records
    *                    OaiError = a previously incurred error.
    *
    * @return Seq[Either[OaiError, OaiRecord]]
    *         OaiRecord - a record appearing in the XML node
    *         OaiError - an error incurred during the process of parsing the records.
    */

  def parseXmlIntoRecords(xmlEither: Either[OaiError, Node]):
    Seq[Either[OaiError, OaiRecord]] = {

    xmlEither match {
      case Left(error) => Seq(Left(error))
      case Right(xml) =>
        for (record <- xml \ "ListRecords" \ "record")
          yield {
            val id = getRecordIdentifier(record)
            val setIds = getSetIdsFromRecord(record)
            val oaiRecord = OaiRecord(id, record.toString, setIds)
            Right(oaiRecord)
          }
    }
  }

  def parseXmlIntoSets(xmlEither: Either[OaiError, Node]):
    Seq[Either[OaiError, OaiSet]] = {

    xmlEither match {
      case Left(error) => Seq(Left(error))
      case Right(xml) =>
        for (set <- xml \ "ListSets" \ "set")
          yield {
            val id = getSetIdentifier(set)
            val oaiSet = OaiSet(id, set.toString)
            Right(oaiSet)
          }
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
  private def parseStringIntoXml(string: String): Try[Node] = Try {
    val xml = XML.loadString(string)
    checkForOaiError(xml)
    xml
  }

  /**
    * Parse an error message from an XML node.
    * Throw an exception if an error is found.
    *
    * @param xml: NodeSeq
    *             The XML that may include an error message.
    * @return Unit
    * @throws RuntimeException
    */
  private def checkForOaiError(xml: NodeSeq): Unit = {
    if ((xml \ "error").nonEmpty)
      throw new RuntimeException((xml \ "error").text.trim)
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

  /**
    * Accepts a set from an OAI feed an returns the OAI identifier
    *
    * @param set Node
    *            The original set from the OAI feed
    * @return The local identifier
    */
  private def getSetIdentifier(set: Node): String = (set \ "setSpec").text
}
