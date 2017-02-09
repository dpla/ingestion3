package la.dp.ingestion3.harvesters

import org.apache.log4j.LogManager

import scala.xml.NodeSeq

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  *
  */
class OaiResponseProcessor() {
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
  def getHarvestedRecords(xml: NodeSeq): List[String] = {
    val records = xml \\ "OAI-PMH" \\ "record"
    records.map(x => x.headOption match {
      case Some(node) => node.toString
      case _ => ""
    }).toList
  }

  /**
    * Get the error property if it exists
    *
    * @return Option[String]
    *         The error code if it exists otherwise empty string
    *
    */
  @throws(classOf[HarvesterException])
  def getOaiErrorCode(xml: NodeSeq): Option[String] = {
    (xml \\ "OAI-PMH" \\ "error").nonEmpty match {
      case true => throw new HarvesterException((xml \\ "OAI-PMH" \\ "error").text.trim)
      case false => None
    }
  }

  /**
    * Get the resumptionToken from the response
    *
    * @param xml NodeSeq
    *            The complete XML response
    * @return Option[String]
    *         The resumptionToken to fetch the next set of records
    *         or None if no more records can be fetched. An
    *         empty string does not mean all records were successfully
    *         harvested (an error could have occured when fetching), only
    *         that there are no more records that can be fetched.
    */
  def getResumptionToken(xml: NodeSeq): Option[String] = {
    (xml \\ "OAI-PMH" \\ "resumptionToken").text match {
      case e if e.isEmpty => None
      case _ => Some( (xml \\ "OAI-PMH" \\ "resumptionToken").text.trim )
    }
  }
}
