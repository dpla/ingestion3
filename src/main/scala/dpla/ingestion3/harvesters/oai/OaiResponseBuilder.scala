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

class OaiResponseBuilder (endpoint: String)
                         (@transient val sqlContext: SQLContext)
  extends Serializable {

  val urlBuilder = new OaiQueryUrlBuilder

  /**
    * Get one to many pages of sets.
    * Results will include all sets.
    */
  def getSets: RDD[OaiSet] = {
    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListSets")
    val response = getMultiPageResponse(baseParams)
    val responseRdd = sqlContext.sparkContext.parallelize(response)
    responseRdd.flatMap(page => parseSets(page))
  }

  /**
    * Get one to many pages of records.
    * Results will include all records, regardless of whether or not they belong
    * to a set.
    * @param opts Optional OAI args, eg. metadataPrefix
    */
  def getRecords(opts: Map[String, String]): RDD[OaiRecord] = {
    val sets = getSets.collect
    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")
    val response = getMultiPageResponse(baseParams, opts)
    val responseRdd = sqlContext.sparkContext.parallelize(response)
    val recordsRdd = responseRdd.flatMap(page => parseRecords(page))

    recordsRdd.flatMap(record => {
      // Get set ids from the record document.
      // Records may have 0 to many sets ids.
      val node = XML.loadString(record.document)
      val setIds = OaiResponseProcessor.getSetIdsFromRecord(node)

      // Map each set id to an OaiSet.
      // Create a new OaiRecord that contains the OaiSet.
      val newRecords = setIds.map(id => {
        val set = sets.filter(s => s.id == id).headOption
        new OaiRecord(record.id, record.document, set)
      })

      // If there are any new OaiRecords with sets, return them.
      // Otherwise, return the original OaiRecord.
      newRecords.size match {
        case 0 => Seq(record)
        case _ => newRecords
      }
    })
  }

  /**
    * Get one to many pages of records from given sets.
    * @param sets Sets from which to harvest records
    * @param opts Optional OAI args, eg. metadataPrefix
    */
  def getRecordsBySets(sets: Array[OaiSet],
                       opts: Map[String, String] = Map()): RDD[OaiRecord] = {

    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")
    val setRdd = sqlContext.sparkContext.parallelize(sets)

    setRdd.flatMap(
      set => {
        val options = opts + ("set" -> set.id)
        val response = getMultiPageResponse(baseParams, options)
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
  def getMultiPageResponse(baseParams: Map[String, String],
                           opts: Map[String, String] = Map()): List[String] = {

    @tailrec
    def loop(data: List[String]): List[String] = {

      val token = OaiResponseProcessor.getResumptionToken(data.head)

      token match {
        case None => data
        case Some(token) => {
          // Resumption tokens are exclusive, meaning a request with a token
          // cannot have any additional optional args.
          val nextParams = baseParams + ("resumptionToken" -> token)
          val nextResponse = getSinglePageResponse(nextParams)
          loop(nextResponse :: data)
        }
      }
    }

    // The initial request must include all optional args.
    val firstParams = baseParams ++ opts
    val firstResponse = getSinglePageResponse(firstParams)
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
