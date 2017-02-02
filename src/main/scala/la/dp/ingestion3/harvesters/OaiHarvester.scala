package la.dp.ingestion3.harvesters

import java.net.URL

import org.apache.log4j.LogManager

import scala.xml.{Elem, Node, NodeSeq, XML}

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  *
  */
class OaiHarvester (xml: NodeSeq) extends Traversable[String] {

  private[this] val logger = LogManager.getLogger("OaiHarvester")

  // Overloaded constructor, needed because I don't instantiate with XML
  // But it needs to be in the class sig for me to pass XML to foreach {}
  // TODO this feels wrong
  def this() = this(Nil)

  /**
    * Traverses the documents in XML result and
    * streams them back as Strings
    *
    * @param f The record harvested
    * @tparam U Xml response from OAI request
    */
  override def foreach[U](f: (String) => U): Unit = {
    // throws exception if error in OAI response
    getOaiErrorCode(xml)

    for (record <- getHarvestedRecords(xml))
      f(record.toString())
  }

  /**
    * Takes the XML response from a ListRecords and returns
    * the <record≥ elements
    *
    * @param xml NodeSeq
    *            Complete OAI-PMH XML response
    *
    * @return Option[NodeSeq]
    *         NodeSeq of records in the response
    */
  def getHarvestedRecords(xml: NodeSeq): Option[NodeSeq] = {
    val records = (xml \\ "OAI-PMH" \\ "record")
    records.headOption match {
      case Some(r) => Some(records)
      case None => None
    }
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
      case true => throw new HarvesterException( (xml \\ "OAI-PMH" \\ "error").text.trim )
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

  /**
    * Executes the request and returns the response
    *
    * @param url URL
    *            OAI request URL
    * @return NodeSeq
    *         XML response
    */
  def getXmlResponse(url: URL): NodeSeq = {
    XML.load(url)
  }
}
