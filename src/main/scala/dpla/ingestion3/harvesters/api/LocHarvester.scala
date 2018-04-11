package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
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
class LocHarvester(shortName: String,
                   conf: i3Conf,
                   outputDir: String,
                   harvestLogger: Logger)
  extends ApiHarvester(shortName, conf, outputDir, harvestLogger) {

  override protected val mimeType: String = "application_json"

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query,
    "c" -> conf.harvest.rows
  ).collect{ case (key, Some(value)) => key -> value } // remove None values

  override protected def localApiHarvest: Unit = {
    implicit val formats = DefaultFormats
    // Get sets from conf
    val collections = conf.harvest.setlist
      .getOrElse(throw new RuntimeException("No sets")).split(",")

    var itemUrls: ListBuffer[URL] = new ListBuffer[URL]()

    collections.foreach( collection => {
      // Mutable vars for controlling harvest loop
      var continueHarvest = true
      var page = "1"

      println(s"Collection: $collection")
      while(continueHarvest) getSinglePage(page, collection) match {
        // Handle errors
        case error: ApiError with ApiResponse =>
          harvestLogger.error("Error returned by request %s\n%s\n%s".format(
            error.errorSource.url.getOrElse("Undefined url"),
            error.errorSource.queryParams,
            error.message
          ))
          continueHarvest = false
        // Handle a successful response
        case src: ApiSource with ApiResponse =>
          src.text match {
            case Some(docs) =>
              // Parse xml to get URLs for items
              val xml = XML.loadString(docs)
              val locItemUrls = xml \\ "url" \\ "loc"

              // Extract URLs from site map and append to itemUrls
              itemUrls = itemUrls ++ locItemUrls
                .filter(url => url.text.contains("www.loc.gov/item/"))
                .map(node => buildItemUrl(node.text))

              // Loop control
              if (locItemUrls.size != queryParams.getOrElse("c", "10").toInt)
                continueHarvest = false
              else
                page = (page.toInt + 1).toString

            case _ =>
              harvestLogger.error(s"Response body is empty.\n" +
                s"URL: ${src.url.getOrElse("!!! URL not set !!!")}\n" +
                s"Params: ${src.queryParams}\n" +
                s"Body: ${src.text}")
              continueHarvest = false
          }
      }
    })

    harvestLogger.info(s"Fetched ${itemUrls.size} urls")
    if(itemUrls.distinct.size != itemUrls.size)
      harvestLogger.info(s"${itemUrls.distinct.size} distinct values. " +
        s"${itemUrls.size-itemUrls.distinct.size} duplicates}")

    val locFetched = fetchLocRecords(itemUrls.distinct)

    // @see ApiHarvester
    saveOutAll(locFetched)
  }

  /**
    *
    * @param urls
    * @return
    */
  def fetchLocRecords(urls: Seq[URL]): List[ApiResponse] = {
    sc.parallelize(urls).map(url => {
      HttpUtils.makeGetRequest(url) match {
        case Failure(e) =>
          ApiError(e.getStackTrace.mkString("\n\t"),
            ApiSource(Map("" -> ""), Option(url.toString), None)  )

        case Success(response) =>
          Try { parse(response) } match {

            case Success(json) => {
              val recordId = (json \\ "item" \\ "id").toString

              if(recordId.nonEmpty)
                ApiRecord(recordId, compact(render(json)))
              else
                ApiError("Missing required property 'id'",
                  ApiSource(Map("" -> ""), Option(url.toString), Option(compact(render(json)))))
            }
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

    harvestLogger.info(s"Requesting ${url.toString}")

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
  override protected def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("http")
      .setHost("www.loc.gov")
      .setPath(s"/collections/${params.getOrElse("collection", throw new RuntimeException("No collection specified"))}")
      .setParameter("c", params.getOrElse("c", "10"))
      .setParameter("fo", "sitemap")
      .setParameter("sp", params.getOrElse("sp", "1"))
      .build()
      .toURL

  /**
    *
    * @param urlStr URL String
    * @return
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
}
