package dpla.ingestion3.harvesters.oai

import org.apache.log4j.LogManager

import scala.util.{Try, Success, Failure}
import scala.xml.{Node, NodeSeq, XML}

/**
  * Basic exception class, minimal implementation
  *
  * @param message String
  */
case class OaiHarvesterException(message: String) extends Exception(message)

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  */
object OaiResponseProcessor {
  private[this] val logger = LogManager.getLogger("OaiHarvester")

  /**
    * Parses records from an OaiSource.
    *
    * @param source OaiSource
    *               The single-page response to a single OAI query.
    *
    * @return OaiResponse
    *         OaiRecordsPage - all the records appearing on the page OR
    *         OaiError - an error incurred during the process of parsing the records.
    */

  def getRecords(source: OaiSource, removeDeleted: Boolean): OaiResponse = {

    // Parse all records from XML
    def getRecordsFromXml(xml: Node): Seq[OaiRecord] = for {
        record <- xml \ "ListRecords" \ "record"
        id = getRecordIdentifier(record)
        setIds = getSetIdsFromRecord(record)
        status = getRecordStatus(record)
        if !removeDeleted || status != "deleted"
      } yield OaiRecord(id, record.toString, setIds, source.copy(text = None))


    val text = source.text.getOrElse("")

    getXml(text) match {
      case Failure(e) => OaiError(e.toString, source)
      case Success(xml) =>
        val records = getRecordsFromXml(xml)
        RecordsPage(records)
    }
  }

  def getRecordStatus(record: Node): String =
    (record \ "header" \ "@status").headOption.getOrElse(<foo>foo</foo>).text


  // TODO: To ensure continuity of IDs between ingestion systems and generalize the ID
  /**
    * Accepts a record from an OAI feed an returns the OAI identifier
    *
    * @param record The original record from the OAI feed
    * @return The local OAI identifier
    */
  def getRecordIdentifier(record: Node): String =
    (record \ "header" \ "identifier").text

  /**
    * Get all set ids from a single record.
    * Return empty Seq if none exist.
    *
    * @return Seq[String]
    *         The set ids.
    */
  def getSetIdsFromRecord(record: Node): Seq[String] =
    for (set <- record \ "header" \ "setSpec") yield set.text

  /**
    * Parses sets from an OaiSource, and filter if necessary according to a
    * whitelist or blacklist.
    *
    * @param source OaiSource
    *               The single-page response to a single OAI query
    *
    * @param setFilter: PartialFunction[(Seq[OaiSet]), Seq[OaiSet])
    *                   A partial function indicating how sets should be filtered,
    *                   ie. according to a whitelist or blacklist.
    */
  def getSets(source: OaiSource,
              setFilter: PartialFunction[(Seq[OaiSet]), Seq[OaiSet]]): OaiResponse = {

    // Parse all sets from XML.
    def getSetsFromXml(xml: Node): Seq[OaiSet] =
      for (set <- xml \ "ListSets" \ "set")
        yield OaiSet(getSetIdentifier(set), set.toString, source)

    val text = source.text.getOrElse("")

    getXml(text) match {
      case Failure(e) => OaiError(e.toString, source)
      case Success(xml) =>
        val sets = getSetsFromXml(xml)
        // Apply partial function to filter out any sets that do not match some criteria.
        val filtered = setFilter(sets)
        SetsPage(filtered)
    }
  }

  /**
    * Accepts a set from an OAI feed an returns the OAI identifier
    *
    * @param set Node
    *            The original set from the OAI feed
    * @return The local identifier
    */
  def getSetIdentifier(set: Node): String = (set \ "setSpec").text

  def getAllSets(page: OaiSource): OaiResponse = {
    val setFilter: PartialFunction[(Seq[OaiSet]), Seq[OaiSet]] = {
      // No filtering required; return all sets.
      case sets => sets
    }
    getSets(page, setFilter)
  }

  // Get all sets that belong to a given whitelist.
  def getSetsByWhitelist(page: OaiSource, whitelist: Array[String]) = {
    val setFilter: PartialFunction[(Seq[OaiSet]), Seq[OaiSet]] = {
      // Return only those sets with ids NOT in the blacklist.
      case sets => sets.filter { set => whitelist.contains(set.id) }
    }
    getSets(page, setFilter)
  }

  // Get all sets except those belonging to a given blacklist.
  def getSetsByBlacklist(page: OaiSource, blacklist: Array[String]) = {
    val setFilter: PartialFunction[(Seq[OaiSet]), Seq[OaiSet]] = {
      // Return only those sets with ids in the whitelist.
      case sets => sets.filterNot{ set => blacklist.contains(set.id) }
    }
    getSets(page, setFilter)
  }

  /**
    * Parse an error message from an XML node.  Throw an excpetion if an error
    * is found.
    *
    * @param xml: NodeSeq
    *             The XML that may include an error message.
    *
    * @return Unit
    */
  def getOaiErrorCode(xml: NodeSeq): Unit = {
    if ((xml \ "error").nonEmpty)
      throw OaiHarvesterException((xml \ "error").text.trim)
  }

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
    * Try to parse XML from a given String, representing a valid OAI response.
    *
    * @param string String
    *               A string response from an OAI feed.
    *
    * @return Try[Node]
    *         An valid XML node, OR
    *         A failure if the XML is invalid or contains an OAI error message.
    */
  def getXml(string: String): Try[Node] = Try {
    val xml = XML.loadString(string)
    getOaiErrorCode(xml)
    xml
  }

  /**
    * Checks the header status property to determine if the record is marked as deleted
    * @param node
    * @return
    */
  def isDeleted(node: NodeSeq): Boolean = {
    (node \ "header").\@("status") match {
      case "deleted" => true
      case _ => false
    }
  }
}
