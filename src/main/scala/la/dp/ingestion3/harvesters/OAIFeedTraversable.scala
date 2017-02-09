package la.dp.ingestion3.harvesters

import java.net.URL

import la.dp.ingestion3.OaiQueryUrlBuilder
import org.apache.log4j.LogManager

import scala.xml.{NodeSeq, XML}

/**
  * Created by DPLA Tech-Team on 2/6/17.
  *
  * Traverses a series of XML requests/responses
  *
  */
class OAIFeedTraversable extends {

  //noinspection ScalaUnusedSymbol
  private[this] val logger = LogManager.getLogger("OaiHarvester")

  /**
    * Tail-recursive implementation of OAI harvest
    *
    * @param params Map[String,String]
    *               The default OAI query parameters
    * @param urlBuilder OaiQueryUrlBuilder
    *                   Builds HTTP requests
    * @return List[NodeSeq]
    *         The XML responses from the harvest
    */
  final def go(params: Map[String, String],
               urlBuilder: OaiQueryUrlBuilder): List[NodeSeq] = {

    @scala.annotation.tailrec
    def goAcc(resumptionToken: String,
              allXml: List[NodeSeq]): List[NodeSeq] = {

      val queryParams = updateParams(List(params, Map("resumptionToken" -> resumptionToken)))
      val url = urlBuilder.buildQueryUrl(queryParams)
      val xml = getXmlResponse(url)
      val rToken = getResumptionToken(xml)

      rToken match {
        case Some(v) => goAcc(rToken.get, allXml :+ xml)
        case None => return allXml
      }
    }
    val allXml = List[NodeSeq]()
    goAcc( "", allXml)
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

  /**
    * Update the query parameters
    *
    * @param listOfParams
    *                     List[ Map[String,String] ]
    *                     A list of Maps to combine
    * @return A single Map
    */
  def updateParams(listOfParams: List[Map[String,String]]): Map[String, String] = {
    listOfParams.flatten.toMap
  }
}
