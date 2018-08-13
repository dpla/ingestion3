
package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success}

/**
  * Class for harvesting records from the California Digital Library's Solr API
  *
  * Calisphere API documentation
  * https://help.oac.cdlib.org/support/solutions/articles/9000101639-calisphere-api
  *
  */
class CdlHarvester(spark: SparkSession,
                   shortName: String,
                   conf: i3Conf,
                   outputDir: String,
                   harvestLogger: Logger)
  extends ApiHarvester(spark, shortName, conf, outputDir, harvestLogger) {

  def mimeType: String = "application_json"

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query,
    "rows" -> conf.harvest.rows,
    "api_key" -> conf.harvest.apiKey
  ).collect{ case (key, Some(value)) => key -> value } // remove None values

  override def localHarvest: Unit = {
    implicit val formats = DefaultFormats

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var cursorMark = "*"

    // Runtime tracking
    val startTime = System.currentTimeMillis()

    while(continueHarvest) getSinglePage(cursorMark) match {
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
            val json = parse(docs)
            val cdlRecords = (json \\ "docs").children.map(doc => {
              ApiRecord((doc \\ "identifier").toString, compact(render(doc)))
            })

            // @see ApiHarvester
            saveOutRecords(cdlRecords)

            // Loop control
            cursorMark = (json \\ "cursorMark").extract[String]
            val nextCursorMark = (json \\ "nextCursorMark").extract[String]

            if (cursorMark.matches(nextCursorMark)) {
              continueHarvest = false
            } else {
              cursorMark = nextCursorMark
            }

          case _ =>
            harvestLogger.error(s"Response body is empty.\n" +
              s"URL: ${src.url.getOrElse("!!! URL not set !!!")}\n" +
              s"Params: ${src.queryParams}\n" +
              s"Body: ${src.text}")
            continueHarvest = false
        }
    }
  }

  /**
    * Get a single-page, un-parsed response from the CDL feed, or an error if
    * one occurs.
    *
    * @param cursorMark Uses cursor and not start/offset to paginate. Used to work around Solr
    *                   deep-paging performance issues.
    * @return ApiSource or ApiError
    */
  private def getSinglePage(cursorMark: String): ApiResponse = {
    val apiKey = queryParams.getOrElse("api_key", "")
    val headers = Some(Map("X-Authentication-Token" -> apiKey))
    val url = buildUrl(queryParams.updated("cursorMark", cursorMark))

    harvestLogger.info(s"Requesting ${url.toString}")

    HttpUtils.makeGetRequest(url, headers) match {
      case Failure(e) =>
        ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
      case Success(response) => response.isEmpty match {
        case true => ApiError("Response body is empty", ApiSource(queryParams, Some(url.toString)))
        case false => ApiSource(queryParams, Some(url.toString), Some(response))
      }
    }
  }

  /**
    * Constructs the URL for CDL API requests
    *
    * @param params URL parameters
    * @return
    */
  def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("https")
      .setHost("solr.calisphere.org")
      .setPath("/solr/query")
      .setParameter("q", params.getOrElse("query", "*:*"))
      .setParameter("cursorMark", params.getOrElse("cursorMark", "*"))
      .setParameter("rows", params.getOrElse("rows", "10"))
      .setParameter("sort", "id desc")
      .build()
      .toURL
}
