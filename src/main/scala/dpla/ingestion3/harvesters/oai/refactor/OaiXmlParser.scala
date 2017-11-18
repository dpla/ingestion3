package dpla.ingestion3.harvesters.oai.refactor

import scala.util.{Failure, Success, Try}
import scala.xml.{Node, NodeSeq, XML}

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  */

// TODO: Refactor to be a class instead of an object
object OaiXmlParser {

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
      case Right(oaiPage) =>
        val xmlTry: Try[Node] = Try { XML.loadString(oaiPage.page) }

        xmlTry match {
          case Failure(e) => Left(OaiError(e.toString))
          case Success(xml) => Right(xml)
        }
    }
  }

  /**
    * Parse XML into OaiRecord, or OaiError if the XML contains an OAI error.
    *
    * @param xmlEither:  Either[OaiError, Node]
    *                    Node - an XML node containing 0-n records
    *                    OaiError = a previously incurred error.
    *
    * @return Seq[Either[OaiError, OaiRecord]]
    *         OaiRecord - a record appearing in the XML node.
    *         OaiError - an error appearing in the XML node, or a previously
    *         incurred error.
    */
  def parseXmlIntoRecords(xmlEither: Either[OaiError, Node]):
    Seq[Either[OaiError, OaiRecord]] = xmlEither match {
      case Left(e) => Seq(Left(e))
      case Right(xml) =>
        getError(xml) match {
          // If the XML contains an error, return an OaiError
          case Some(e) => Seq(Left(e))
          // Otherwise, parse records from the XML
          case None => getRecords(xml).map(Right(_))
        }
    }

  /**
    * Parse XML into OaiSet, or OaiError if the XML contains an OAI error.
    *
    * @param xmlEither:  Either[OaiError, Node]
    *                    Node - an XML node containing 0-n sets.
    *                    OaiError = a previously incurred error.
    *
    * @return Seq[Either[OaiError, OaiSet]]
    *         OaiSet - a set appearing in the XML node.
    *         OaiError - an error appearing in the XML node, or a previously
    *         incurred error.
    */
  def parseXmlIntoSets(xmlEither: Either[OaiError, Node]):
    Seq[Either[OaiError, OaiSet]] = xmlEither match {
      case Left(e) => Seq(Left(e))
      case Right(xml) =>
        getError(xml) match {
          // If the XML contains an error, return an OaiError
          case Some(e) => Seq(Left(e))
          // Otherwise, parse records from the XML
          case None => getSets(xml).map(Right(_))
        }
    }

  def getRecords(xml: Node): Seq[OaiRecord] =
    for (record <- xml \ "ListRecords" \ "record")
      yield {
        val id = (record \ "header" \ "identifier").text
        val setIds = for (set <- record \ "header" \ "setSpec") yield set.text
        OaiRecord(id, record.toString, setIds)
      }

  def getSets(xml: Node): Seq[OaiSet] =
    for (set <- xml \ "ListSets" \ "set")
      yield {
        val id = (set \ "setSpec").text
        OaiSet(id, set.toString)
      }

  def getError(xml: Node): Option[OaiError] = {
    val error = (xml \ "error")
    if (error.nonEmpty) Some(OaiError(error.text.trim)) else None
  }
}
