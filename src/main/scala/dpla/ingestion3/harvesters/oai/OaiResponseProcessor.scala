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

  /**
    * Takes the XML response from a ListRecords and returns
    * the <record≥ elements
    *
    * @param xml NodeSeq
    *            Complete OAI-PMH XML response
    *
    * @return List[String]
    *         NodeSeq of records in the response
    */
  def getRecordsAsList(xml: NodeSeq): List[String] = {
    val records = xml \\ "OAI-PMH" \\ "record"
    records.map(x => x.headOption match {
      case Some(node) => node.toString
    }).toList
  }

  /**
    * Iterates through the records in an OAI response and generates a DPLA ID
    * for each and maps the original record to the DPLA ID
    *
    * @param xml NodeSeq
    *            The XML response from the OAI request
    * @return
    */
  def getRecordsAsTuples(xml: NodeSeq): Seq[(String,String)] = {
    val records = xml \\ "OAI-PMH" \\ "record"
    records.map(r => r.headOption match {
      case Some(node) => {
        val localId = Harvester.getLocalId(node)
        (Harvester.generateMd5(localId), node.toString)
      }
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
