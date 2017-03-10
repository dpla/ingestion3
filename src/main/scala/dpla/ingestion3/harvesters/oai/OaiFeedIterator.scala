package dpla.ingestion3.harvesters.oai

import java.net.URL

import dpla.ingestion3.harvesters.OaiQueryUrlBuilder
import org.apache.log4j.LogManager

import scala.collection.mutable.Stack
import scala.xml.{NodeSeq, XML}


class OaiFeedIterator(params: Map[String,String], urlBuilder: OaiQueryUrlBuilder)
  extends Iterator[(String,String)] {

  private[this] val logger = LogManager.getLogger(this.getClass)

  private[this] val buffer = new Stack[(String,String)]

  private[this] var resumptionToken = Option("")

  override def hasNext: Boolean = {
    if (buffer.isEmpty) fillBuffer
    resumptionToken.isDefined
  }

  override def next(): (String, String) = {
    if(buffer.isEmpty) fillBuffer
    buffer.pop()
  }

  /**
    *
    * @return
    */
  private[this] def fillBuffer(): Unit = {
    val oaiProcessor = new OaiResponseProcessor()
    val queryParams = updateParams(List(params, Map("resumptionToken" -> resumptionToken.get)))
    val url = urlBuilder.buildQueryUrl(queryParams)
    val xml = getXmlResponse(url)
    val recordsMap = oaiProcessor.getRecordsAsMap(xml)
    resumptionToken = oaiProcessor.getResumptionToken(xml)
    buffer.pushAll(recordsMap)
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
