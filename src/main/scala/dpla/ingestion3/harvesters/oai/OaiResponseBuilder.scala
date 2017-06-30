package dpla.ingestion3.harvesters.oai

import java.net.URL
import java.nio.charset.Charset
import dpla.ingestion3.harvesters.OaiQueryUrlBuilder
import org.apache.http.client.fluent.Request
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import scala.annotation.tailrec
import scala.xml.XML

/**
 * This class handles requests to the OAI feed.
 * It partitions data at strategic points.
 */
class OaiResponseBuilder (baseParams: Map[String, String])
                         (@transient val sqlContext: SQLContext)
  extends Serializable {

  val urlBuilder = new OaiQueryUrlBuilder

  def getSets: RDD[OaiSet] = {
    val response = getMultiPageResponse(baseParams)
    val responseRdd = sqlContext.sparkContext.parallelize(response)
    responseRdd.flatMap(page => parseSets(page))
  }

  def getRecords: RDD[OaiRecord] = {
    val response = getMultiPageResponse(baseParams)
    val responseRdd = sqlContext.sparkContext.parallelize(response)
    responseRdd.flatMap(page => parseRecords(page))
  }

  // Get one to many pages of records from given sets.
  def getRecordsBySets(sets: Array[OaiSet]): RDD[OaiRecord] = {
    val setRdd = sqlContext.sparkContext.parallelize(sets)

    setRdd.flatMap(
      set => {
        val params = baseParams + ("set" -> set.id)
        val response = getMultiPageResponse(params)
        response.flatMap(page => parseRecords(page, Some(set)))
      }
    )
  }

  /**
    * Get all pages of results from an OAI feed.
    * Makes an initial call to the feed to get the first page of results.
    * For this and all subsequent pages, calls the next page if a resumption
    * token is present.
    * Returns a single List of single-page responses as Strings.
    */
  def getMultiPageResponse(parameters: Map[String, String]): List[String] = {

    @tailrec
    def loop(data: List[String]): List[String] = {

      val token = OaiResponseProcessor.getResumptionToken(data.head)

      token match {
        case None => data
        case Some(token) => {
          val queryParams = parameters + ("resumptionToken" -> token)
          val nextResponse = getSinglePageResponse(queryParams)
          loop(nextResponse :: data)
        }
      }
    }

    val firstResponse = getSinglePageResponse(parameters)
    loop(List(firstResponse))
  }

  // Returns a single page response as a single String
  def getSinglePageResponse(queryParams: Map[String, String]): String = {
    val url = urlBuilder.buildQueryUrl(queryParams)
    println(url) // for testing purposes, can delete later
    getStringResponse(url)
  }

  /**
    * Executes the request and returns the response
    *
    * @param url URL
    *            OAI request URL
    * @return String
    *         String response
    *
    *         OAI-PMH XML responses must be enncoded as UTF-8.
    * @see https://www.openarchives.org/OAI/openarchivesprotocol.html#XMLResponse
    *
    *      TODO: Handle failed HTTP request.
    */

  def getStringResponse(url: URL) : String = {
    Request.Get(url.toURI)
      .execute()
      .returnContent()
        .asString(Charset.forName("UTF8"))
  }

  def parseSets(page: String): Seq[OaiSet] = {
    val xml = XML.loadString(page)
    OaiResponseProcessor.getSets(xml)
  }

  def parseRecords(page: String, set: Option[OaiSet] = None): Seq[OaiRecord] = {
    val xml = XML.loadString(page)
    OaiResponseProcessor.getRecords(xml, set)
  }
}
