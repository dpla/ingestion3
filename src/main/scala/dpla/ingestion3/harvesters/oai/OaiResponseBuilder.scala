package dpla.ingestion3.harvesters.oai

import java.net.URL
import java.nio.charset.Charset

import dpla.ingestion3.harvesters.UrlBuilder
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.fluent.Request
import org.apache.http.client.utils.URIBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * This class handles requests to the OAI feed.
 * It partitions data at strategic points.
 */

class OaiResponseBuilder (endpoint: String)
                         (@transient val sqlContext: SQLContext)
  extends Serializable with UrlBuilder {

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
  def getAllRecords(opts: Map[String, String], removeDeleted: Boolean): RDD[OaiResponse] = {
    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")
    val multiPageResponse = getMultiPageResponse(baseParams, opts)
    val responseRdd = sqlContext.sparkContext.parallelize(multiPageResponse)
    val recordsRdd = responseRdd.map(response => parseRecordsResponse(response, removeDeleted))
    recordsRdd.union(getAllSets)
  }

  /**
    * Get one to many pages of records from given sets.
    * @param setResponses Set pages containing sets from which to harvest records
    *                     or errors incurred during the process of harvesting sets.
    * @param opts Optional OAI args, eg. metadataPrefix
    */
  def getRecordsBySets(setResponses: RDD[OaiResponse],
                       opts: Map[String, String] = Map(), removeDeleted: Boolean): RDD[OaiResponse] = {

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
        multiPageResponse.map(response => parseRecordsResponse(response, removeDeleted))
      }
    )

    // Return all sets, records, and errors.
    records.union(setResponses)
  }

  def parseRecordsResponse(response: OaiResponse, removeDeleted: Boolean): OaiResponse = {
    response match {
      case page: OaiSource => OaiResponseProcessor.getRecords(page, removeDeleted)
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
        // Stops the harvest if an OaiError or Http error was trapped and returns everything
        // harvested up that this point plus the error
        case Some(error: OaiError) => data
        // If it was a valid and parsable response then extract data and call the next page
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
        // This is only reached if something really strange happened
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
      case Success(url) => {
        HttpUtils.makeGetRequest(url) match {
          // HTTP error
          case Failure(e) =>
            val source = OaiSource(queryParams, Some(url.toString))
            OaiError(e.toString, source)
            // If a successful HTTP request was made we still need to parse the response to determine if there was
            // and OAI error (HTTP code 200) or invalid XML that will prevent additional requests.
            // If the XML can be parsed and if there was no OAI error then it will return Success otherwise Failure
          case Success(response) => OaiResponseProcessor.getXml(response) match {
            case Success(_) =>
              OaiSource(queryParams, Some(url.toString), Some(response))
            case Failure(e) => OaiError(e.toString, OaiSource(queryParams, Some(url.toString), Some(response)))
          }
        }
      }
    }
  }

  /**
    * Tries to build a URL from the parameters
    *
    * @param queryParams HTTP parameters
    * @return Try[URL]
    */
  def getUrl(queryParams: Map[String, String]): Try[URL] = Try { buildUrl(queryParams) }

  /**
    * Builds an OAI request
    *
    * @param params Map of parameters needed to construct the URL
    *               OAI request verbs
    *               See https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
    * @return URL
    */
  override def buildUrl(params: Map[String, String]): URL = {
    // Required properties, not sure if this is the right style
    assume(params.get("endpoint").isDefined)
    assume(params.get("verb").isDefined)
    val url = new URL(params("endpoint"))
    val verb = params("verb")

    // Optional properties.
    val metadataPrefix: Option[String] = params.get("metadataPrefix")
    val resumptionToken: Option[String] = params.get("resumptionToken")
    val set: Option[String] = params.get("set")

    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPort(url.getPort)
      .setPath(url.getPath)
      .setParameter("verb", verb)

    // Set optional properties.
    resumptionToken.foreach(t => urlParams.setParameter("resumptionToken", t))
    set.foreach(s => urlParams.setParameter("set", s))
    metadataPrefix.foreach(prefix => urlParams.setParameter("metadataPrefix", prefix))

    urlParams.build.toURL
  }

}
