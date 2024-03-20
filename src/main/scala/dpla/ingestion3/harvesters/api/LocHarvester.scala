package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import scala.xml.XML

/**
  * Class for harvesting records from the Library of Congress's API
  *
  * API documentation
  * https://libraryofcongress.github.io/data-exploration/
  *
  */
class LocHarvester(spark: SparkSession,
                   shortName: String,
                   conf: i3Conf,
                   harvestLogger: Logger)
  extends ApiHarvester(spark, shortName, conf, harvestLogger) {

  def mimeType: String = "application_json"

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query,
    "c" -> conf.harvest.rows
  ).collect{ case (key, Some(value)) => key -> value } // remove None values

  /**
    * Entry method for invoking LC harvest
    */
  override def localHarvest(): DataFrame = {
    implicit val formats = DefaultFormats
    // Get sets from conf
    val collections = conf.harvest.setlist
      .getOrElse(throw new RuntimeException("No sets")).split(",")

    var itemUrls: ListBuffer[URL] = new ListBuffer[URL]()

    collections.foreach( collection => {
      // Mutable vars for controlling harvest loop
      var continueHarvest = true
      var page = "1"

      harvestLogger.info(s"Processing sitemaps for collection: $collection")

      while(continueHarvest) getSinglePage(page, collection) match {
        // Handle errors
        case error: ApiError with ApiResponse =>
          logError(error)
          continueHarvest = false
        // Handle a successful response
        case src: ApiSource with ApiResponse => src.text match {
          case Some(docs) =>
            val xml = XML.loadString(docs)
            val locItemNodes = xml \\ "url" \\ "loc"
            // Extract URLs from site map, filter out non-harvestable URLs and
            // append valid URLs to itemUrls List
            itemUrls = itemUrls ++ locItemNodes
              .filter(url => url.text.contains("www.loc.gov/item/"))
              .map(node => buildItemUrl(node.text))
            // Loop control
            if (locItemNodes.size != queryParams.getOrElse("c", "10").toInt)
              continueHarvest = false
            else
              page = (page.toInt + 1).toString
          // Handle empty response from API
          case _ =>
            logError(ApiError("Response body is empty", src))
            continueHarvest = false
        }
        case _ => throw new RuntimeException("Not sure how we got here!")

      }
    })
    // Log results of item gathering
    harvestLogger.info(s"Fetched ${itemUrls.size} urls")
    if(itemUrls.distinct.size != itemUrls.size)
      harvestLogger.info(s"${itemUrls.distinct.size} distinct values. " +
        s"${itemUrls.size-itemUrls.distinct.size} duplicates}")
    // Fetch items
    val locFetched = fetchLocRecords(itemUrls.distinct.toSeq)

    // @see ApiHarvester
    saveOutAll(locFetched)

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }

  /**
    * Fetch all items from LC
    *
    * @param urls Seq[URL] URLs to fetch
    * @return List[ApiResponse] List containing ApiErrors and ApiRecords
    */
  def fetchLocRecords(urls: Seq[URL]): List[ApiResponse] = {
    spark.sparkContext.parallelize(urls).map(url => {
      HttpUtils.makeGetRequest(url) match {
        case Failure(e) =>
          val urlString = Try { url.toString } match {
            case Success(urlStr) => Some(urlStr)
            case _ => None
          }
          ApiError(e.getStackTrace.mkString("\n\t"),
            ApiSource(Map("" -> ""), urlString, None)  )
        case Success(response) =>
          Try { parse(response) } match {
            case Success(json) =>
              val recordId = (json \\ "item" \\ "id").toString
              if(recordId.nonEmpty)
                ApiRecord(recordId, compact(render(json)))
              else
                ApiError("Missing required property 'id'",
                  ApiSource(Map("" -> ""), Option(url.toString), Option(compact(render(json)))))
            case Failure(parseError) =>
              ApiError(parseError.getStackTrace.mkString("\n\t"),
                ApiSource(Map("" -> ""), Option(url.toString), Option(response))  )
          }

      }
    }).collect().toList
  }

  /**
    * Get a single-page, un-parsed response from the CDL feed, or an error if
    * one occurs.
    *
    * @param sp Pagination
    * @param collection Name of collection
    * @return ApiSource or ApiError
    */
  private def getSinglePage(sp: String, collection: String): ApiResponse = {
    val url = buildUrl(queryParams.updated("sp", sp).updated("collection", collection))
    HttpUtils.makeGetRequest(url) match {
      case Failure(e) =>
        ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
      case Success(response) => if (response.isEmpty) {
        ApiError("Response body is empty", ApiSource(queryParams, Some(url.toString)))
      } else {
        ApiSource(queryParams, Some(url.toString), Some(response))
      }
    }
  }

  /**
    * Constructs the URL for collection sitemamp requests
    *
    * @param params URL parameters
    * @return
    */
  def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("http")
      .setHost("www.loc.gov")
      .setPath(s"/collections/${params.getOrElse("collection",
        throw new RuntimeException("No collection specified"))}")
      .setParameter("c", params.getOrElse("c", "10"))
      .setParameter("fo", "sitemap")
      .setParameter("sp", params.getOrElse("sp", "1"))
      .build()
      .toURL

  /**
    * Constructs a URL to request the JSON view of an item in LC's API
    *
    * @param urlStr String
    * @return URL
    */
  def buildItemUrl(urlStr: String): URL = {
    val url = new URL(urlStr)

    new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPath(url.getPath)
      .setParameter("fo", "json")
      .setParameter("at", "item")
      .build()
      .toURL
  }

  /**
    * Log error messages
    *
    * @param error ApiError
    * @param msg Error message
    */
  def logError(error: ApiError, msg: Option[String] = None): Unit = {
    harvestLogger.error("%s  %s\n%s\n%s".format(
      msg.getOrElse("URL: "),
      error.errorSource.url.getOrElse("!!! Undefined URL !!!"),
      error.errorSource.queryParams,
      error.message
    ))
  }
}
