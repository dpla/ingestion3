package dpla.ingestion3.harvesters.oai

import java.net.URL
import java.nio.charset.Charset

import dpla.ingestion3.harvesters.OaiQueryUrlBuilder
import org.apache.http.client.fluent.Request
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.annotation.tailrec

/**
 * This class handles requests to the OAI feed.
 * It partitions data at strategic points.
 */
class OaiResponseBuilder (@transient val sqlContext: SQLContext)
  extends Serializable {

  val urlBuilder = new OaiQueryUrlBuilder

  // Get one to many pages of records.
  def getResponse(oaiParams: Map[String, String]): RDD[String] = {
    val response = getMultiPageResponse(oaiParams)
    sqlContext.sparkContext.parallelize(response)
  }

  // Get one to many pages of records from given sets.
  def getResponseBySets(baseParams: Map[String, String],
                    sets: Array[String]): RDD[String] = {

    val rdd = sqlContext.sparkContext.parallelize(sets)

    val response: RDD[List[String]] = rdd.map(
      set => {
        val oaiParams = baseParams + ("set" -> set)
        getMultiPageResponse(oaiParams)
      }
    )

    response.flatMap(x => x)
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
}
