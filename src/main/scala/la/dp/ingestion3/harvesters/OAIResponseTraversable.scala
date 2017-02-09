package la.dp.ingestion3.harvesters

import org.apache.log4j.LogManager

import scala.xml.NodeSeq

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  *
  */
class OAIResponseTraversable(xml: NodeSeq) extends Traversable[String] {
  private[this] val logger = LogManager.getLogger("OaiHarvester")

  /**
    * Traverses the documents in XML result and
    * streams them back as Strings
    *
    * @param f The record harvested
    * @tparam U Xml response from OAI request
    */
  override def foreach[U](f: (String) => U): Unit = {
    getHarvestedRecords(xml) match {
      case Some(records) => {
        for (record <- records)
          f(record.toString())
      }
      case _ => logger.warn("No records to process")
    }
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
    val records = xml \\ "OAI-PMH" \\ "record"
    records.headOption match {
      case Some(r) => Some(records)
      case None => None
    }
  }
}
