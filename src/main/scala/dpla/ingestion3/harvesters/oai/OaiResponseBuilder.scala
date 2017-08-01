package dpla.ingestion3.harvesters.oai

import java.net.URL
import java.nio.charset.Charset

import dpla.ingestion3.harvesters.OaiQueryUrlBuilder

import org.apache.http.client.fluent.Request
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.annotation.tailrec
import scala.util.{Try, Success, Failure}

/**
 * This class handles requests to the OAI feed.
 * It partitions data at strategic points.
 */

class OaiResponseBuilder (endpoint: String)
                         (@transient val sqlContext: SQLContext)
  extends Serializable {

  val urlBuilder = new OaiQueryUrlBuilder

  /**
    * Get one to many pages of sets, and any errors that occurred in the process
    * of harvesting said sets.
    *
    * @param pageProcessor a parital function that indicates how a response from
    *                      the OAI feed should be processed, e.g. if sets should
    *                      be parsed according to a whitelist or blacklist.
    *
    * @return RDD of OaiResponses, including SetsPages and OaiErrors.
    */
  def getSets(pageProcessor: PartialFunction[(OaiSource), OaiResponse]): RDD[OaiResponse] = {
    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListSets")
    val multiPageResponse = getMultiPageResponse(baseParams)
    val responseRdd = sqlContext.sparkContext.parallelize(multiPageResponse)

    responseRdd.map{
      // Apply partial function to process the OaiSource in some specific way.
      case page: OaiSource => pageProcessor(page)
      // Retain other responses (ie. errors) as given.
      case response: OaiResponse => response
    }
  }

  def getAllSets(): RDD[OaiResponse] = {
    val allSetsPf: PartialFunction[(OaiSource), OaiResponse] = {
      case page => OaiResponseProcessor.getAllSets(page)
    }
    getSets(allSetsPf)
  }


  // Get only those sets included in a given whitelist.
  def getSetsInclude(setIds: Array[String]) = {
    val whitelistPf: PartialFunction[(OaiSource), OaiResponse] = {
      case page => OaiResponseProcessor.getSetsByWhitelist(page, setIds)
    }
    getSets(whitelistPf)
  }

  // Get all sets except those included in a given blacklist.
  def getSetsExclude(setIds: Array[String]) = {
    val blacklistPf: PartialFunction[(OaiSource), OaiResponse] = {
      case page => OaiResponseProcessor.getSetsByBlacklist(page, setIds)
    }
    getSets(blacklistPf)
  }

  /**
    * Get one to many pages of records, and any errors incurred during the
    * process of harvesting said records.
    * Results will include all records, regardless of whether or not they belong
    * to a set.
    * @param opts Optional OAI args, eg. metadataPrefix
    */
  def getAllRecords(opts: Map[String, String]): RDD[OaiResponse] = {
    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")
    val multiPageResponse = getMultiPageResponse(baseParams, opts)
    val responseRdd = sqlContext.sparkContext.parallelize(multiPageResponse)
    val recordsRdd = responseRdd.map(response => parseRecordsResponse(response))
    recordsRdd.union(getAllSets)
  }

  /**
    * Get one to many pages of records from given sets.
    * @param setResponses Set pages containing sets from which to harvest records
    *                     or errors incurred during the process of harvesting sets.
    * @param opts Optional OAI args, eg. metadataPrefix
    */
  def getRecordsBySets(setResponses: RDD[OaiResponse],
                       opts: Map[String, String] = Map()): RDD[OaiResponse] = {

    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")

    // Get ids for all sets (any any errors incurred in process).
    val setIds: RDD[String] = setResponses.flatMap {
      case page: SetsPage => page.sets.map(_.id)
      // For OaiErrors, etc. return an empty sequence.
      case _ => Seq()
    }

    // Repartition ids so they are evenly distributed across clusters.
    val repartitioned = setIds.repartition(setIds.partitions.size)

    /**
      * Get all records for each set (and any errors incurred in process).
      * Any single record may appear in multiple sets.
      *
      * @return RDD of OaiResponses, including RecordsPages and OaiErrors.
      */
    val records: RDD[OaiResponse] = repartitioned.flatMap(
      setId => {
        val options = opts + ("set" -> setId)
        val multiPageResponse = getMultiPageResponse(baseParams, options)
        multiPageResponse.map(response => parseRecordsResponse(response))
      }
    )

    // Return all sets, records, and errors.
    records.union(setResponses)
  }

  def parseRecordsResponse(response: OaiResponse): OaiResponse = {
    response match {
      case page: OaiSource => OaiResponseProcessor.getRecords(page)
      case _ => response
    }
  }

  /**
    * Get all pages of results from an OAI feed.
    * Makes an initial call to the feed to get the first page of results.
    * For this and all subsequent pages, calls the next page if a resumption
    * token is present.
    *
    * @return Un-parsed response page from OAI requests, including OaiSources
    *         and OaiErrors.
    */
  def getMultiPageResponse(baseParams: Map[String, String],
                           opts: Map[String, String] = Map()): List[OaiResponse] = {

    @tailrec
    def loop(data: List[OaiResponse]): List[OaiResponse] = {

      data.headOption match {

        case Some(previous: OaiSource) =>
          val text = previous.text.getOrElse("")
          val token = OaiResponseProcessor.getResumptionToken(text)

          token match {
            case None => data
            case Some(token) =>
              // Resumption tokens are exclusive, meaning a request with a token
              // cannot have any additional optional args.
              val nextParams = baseParams + ("resumptionToken" -> token)
              val nextResponse = getSinglePageResponse(nextParams)
              loop(nextResponse :: data)
          }
        // If there is an error or unexpected response type, return all data
        // collected up to this point (including the error or unexpected response).
        case _ => data
      }
    }

    // The initial request must include all optional args.
    val firstParams = baseParams ++ opts
    val firstResponse = getSinglePageResponse(firstParams)
    loop(List(firstResponse))
  }

  /**
    * Get a single-page, unparsed response from the OAI feed, or an error if
    * one occurs.
    *
    * @param queryParams parameters for a single OAI request.
    * @return OaiSource or OaiError
    */
  def getSinglePageResponse(queryParams: Map[String, String]): OaiResponse = {

    getUrl(queryParams) match {
      // Error building URL
      case Failure(e) =>
        val source = OaiSource(queryParams)
        OaiError(e.toString, source)
      case Success(url) =>
        getStringResponse(url) match {
          // HTTP error
          case Failure(e) =>
            val source = OaiSource(queryParams, Some(url.toString))
            OaiError(e.toString, source)
          case Success(response) =>
            OaiSource(queryParams, Some(url.toString), Some(response))
        }
    }
  }

  // Attempts to build a URL based on given query params.
  def getUrl(queryParams: Map[String, String]): Try[URL] = Try {
    urlBuilder.buildQueryUrl(queryParams)
  }

  /**
    * Executes the request and returns the response
    *
    * @param url URL
    *            OAI request URL
    * @return Try[String]
    *         String response
    *
    *         OAI-PMH XML responses must be enncoded as UTF-8.
    * @see https://www.openarchives.org/OAI/openarchivesprotocol.html#XMLResponse
    */
  def getStringResponse(url: URL) : Try[String] = Try {
    Request.Get(url.toURI)
      .execute()
      .returnContent()
      .asString(Charset.forName("UTF8"))
  }
}
