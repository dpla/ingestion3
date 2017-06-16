package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.harvesters.{Harvester, HarvesterException}
import org.apache.log4j.LogManager

import scala.xml.NodeSeq

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  *
  */
object OaiResponseProcessor {
  private[this] val logger = LogManager.getLogger("OaiHarvester")

  /*
   * Get records or sets, depending on verb.
   * Verbs in OAI-PMH are case sensitive.
   */
  def getItems(xml: NodeSeq, verb: String): Seq[(String, String)] = {
    verb match {
      case "ListRecords" => getRecords(xml)
      case "ListSets" => getSets(xml)
      case _ => throw new RuntimeException("Verb '" + verb + "' not recognized")
    }
  }

  /**
    * Iterates through the records in an OAI response and generates a DPLA ID
    * for each and maps the original record to the DPLA ID
    *
    * @param xml NodeSeq
    *            The XML response from the OAI request
    * @return
    */
  def getRecords(xml: NodeSeq): Seq[(String,String)] = {
    val records = xml \\ "OAI-PMH" \\ "record"
    records.map(r => r.headOption match {
      case Some(node) => {
        val localId = Harvester.getLocalId(node)
        (Harvester.generateMd5(localId), node.toString)
      }
      case None => throw new RuntimeException("XML parsing error")
    })
  }

  /**
    * Iterates through the sets in an OAI response.
    * Returns the setSpec ID and the full text of the record as Strings.
    *
    * @param xml NodeSeq
    *            The XML response from the OAI request
    */
  def getSets(xml: NodeSeq): Seq[(String, String)] = {
    val sets = xml \\ "OAI-PMH" \\ "set"
    sets.map(s => s.headOption match {
      case Some(node) => {
        val id = node \\ "setSpec"
        (id.text, node.toString)
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
  @throws(classOf[HarvesterException])
  def getOaiErrorCode(xml: NodeSeq): Unit = {
    (xml \\ "OAI-PMH" \\ "error").nonEmpty match {
      case true => throw new HarvesterException((xml \\ "OAI-PMH" \\ "error").text.trim)
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
}
