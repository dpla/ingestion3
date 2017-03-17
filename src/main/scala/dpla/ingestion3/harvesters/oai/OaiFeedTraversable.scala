package dpla.ingestion3.harvesters.oai

import java.net.URL

import dpla.ingestion3.harvesters.OaiQueryUrlBuilder
import org.apache.log4j.LogManager

import scala.xml.{NodeSeq, XML}

/**
  * Created by DPLA Tech-Team on 2/6/17.
  *
  * Traverses a series of XML requests/responses
  *
  * @param params Map[String,String]
  *               The default OAI query parameters
  * @param urlBuilder OaiQueryUrlBuilder
  *                   Builds HTTP requests
  */
class OaiFeedTraversable(params: Map[String,String],
                         urlBuilder: OaiQueryUrlBuilder)
  extends Traversable[(String,String)] {

  //noinspection ScalaUnusedSymbol
  private[this] val logger = LogManager.getLogger("OaiHarvester")

  /**
    * Tail-recursive implementation of OAI harvest
    *
    * @return Streams Strings back to caller
    *
    */
  override def foreach[U](f: ((String,String)) => U): Unit = {

    @scala.annotation.tailrec
    def foreachAccumulator(resumptionToken: String,
              f: ( (String,String) ) => U): Unit = {

      val oaiProcessor = new OaiResponseProcessor()
      val queryParams = updateParams(List(params, Map("resumptionToken" -> resumptionToken)))
      val url = urlBuilder.buildQueryUrl(queryParams)
      val xml = getXmlResponse(url)
      val recordsMap = oaiProcessor.getRecordsAsMap(xml)
      val rToken  = oaiProcessor.getResumptionToken(xml)
      // Stream records back
      recordsMap.foreach( f(_))

      rToken match {
        case Some(r) => foreachAccumulator(r, f)
        case None => Unit
      }
    }
    foreachAccumulator("", f)
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
