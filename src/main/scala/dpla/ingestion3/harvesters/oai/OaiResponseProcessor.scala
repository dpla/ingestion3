package dpla.ingestion3.harvesters.oai

import org.apache.log4j.LogManager

import scala.xml.{Node, NodeSeq}

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
    * Iterates through the records in an OAI response.
    * Returns a Sequence of OaiRecord
    *
    * @param xml NodeSeq
    *            The XML response from the OAI request
    */

    def getRecords(xml: NodeSeq, set: Option[OaiSet] = None): Seq[OaiRecord] = {
      val records = xml \\ "OAI-PMH" \\ "record"
      records.map(r => r.headOption match {
        case Some(node) => {
          new OaiRecord(getOaiIdentifier(node), node.toString, set)
        }
        case None => throw new RuntimeException("XML parsing error")
      })
    }

  /**
    * Iterates through the sets in an OAI response.
    * Returns a Sequence of OaiSet
    *
    * @param xml NodeSeq
    *            The XML response from the OAI request
    */
    def getSets(xml: NodeSeq): Seq[OaiSet] = {
      val sets = xml \\ "OAI-PMH" \\ "set"
      sets.map(s => s.headOption match {
        case Some(node) => {
          val id = node \\ "setSpec"
          new OaiSet(id.text, node.toString)
        }
        case None => throw new RuntimeException("XML parsing error")
      })
    }

  /**
    * Get the error property if it exists
    *
    * @return Option[String]
    *         The error code if it exists otherwise empty string
    *
    */
  @throws(classOf[OaiHarvesterException])
  def getOaiErrorCode(xml: NodeSeq): Unit = {
    (xml \\ "OAI-PMH" \\ "error").nonEmpty match {
      case true => throw new OaiHarvesterException((xml \\ "OAI-PMH" \\ "error").text.trim)
      case false => Unit
    }
  }

  /**
    * Get the resumptionToken from the response
    *
    * @param string String
    *            The complete XML response
    * @return Option[String]
    *         The resumptionToken to fetch the next set of records
    *         or None if no more records can be fetched. An
    *         empty string does not mean all records were successfully
    *         harvested (an error could have occurred when fetching), only
    *         that there are no more records that can be fetched.
    */
  def getResumptionToken(string: String): Option[String] = {
    val pattern = """<resumptionToken.*>(.*)</resumptionToken>""".r
    pattern.findFirstMatchIn(string) match {
      case Some(m) => Some(m.group(1))
      case None => None
    }
  }

  // TODO: To ensure continuity of IDs between ingestion systems and generalize the ID
  // TODO: selection between records and sets we need to parameterize the path to the ID.
  /**
    * Accepts a record from an OAI feed an returns the OAI identifier
    *
    * @param record String
    *               The original record from the OAI feed
    * @param path String
    *             XML path to the ID, defaults to header/identifier
    * @return The local identifier
    */
  def getOaiIdentifier(record: Node, path: String = "header/identifier"): String = {
    (record \\ "header" \\ "identifier").text
  }
}
