
package dpla.ingestion3.harvesters.api

import java.net.URL

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success}

/**
  * Digital Library of Georgia API harvester
  * endpoint: https://dlgadmin.galileo.usg.edu/dpla
  *
  * Set a header 'X-User-Token' to API key value
  *
  * nextCursorMark is returned in the response and can be set as a request param (as "cursormark"),
  * in addition to rows (as "rows"). Rows defaults to 1000
  *
  * No query is required as we consume the entire feed. The only required request parameter is
  * `cursormark` and the authorization token passed via the `X-User-Token` header.
  *
  */
class DlgHarvester(spark: SparkSession,
                   shortName: String,
                   conf: i3Conf,
                   harvestLogger: Logger)
  extends ApiHarvester(spark, shortName, conf, harvestLogger) {

  def mimeType: String = "application_json"

  override protected val queryParams: Map[String, String] = Map(
    "rows" -> conf.harvest.rows,
    "api_key" -> conf.harvest.apiKey
  ).collect{ case (key, Some(value)) => key -> value } // remove None values

  override def localHarvest: DataFrame = {
    implicit val formats = DefaultFormats

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var cursorMark = ""

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
            val cdlRecords = (json \\ "items").children.map(doc => {
              ApiRecord((doc \\ "id").toString, compact(render(doc)))
            })

            // @see ApiHarvester
            saveOutRecords(cdlRecords)

            // Loop control
            val nextCursorMark = (json \\ "nextCursorMark").extract[String]

            harvestLogger.info(s"cursorMark == $nextCursorMark")

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
    // Read harvested data into Spark DataFrame and return.
    spark.read.avro(tmpOutStr)
  }

  /**
    * Get a single-page, un-parsed response from the DLG feed, or an error if
    * one occurs.
    *
    * @param cursorMark Uses cursor and not start/offset to paginate. Used to work around Solr
    *                   deep-paging performance issues.
    * @return ApiSource or ApiError
    */
  private def getSinglePage(cursorMark: String): ApiResponse = {
    val apiKey = queryParams.getOrElse("api_key", "")
    val headers = Some(Map("X-User-Token" -> apiKey))
    val url = buildUrl(queryParams.updated("cursorMark", cursorMark).filter(_._2.nonEmpty))

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
    * Constructs the URL for DLG API requests
    *
    * https://dlgadmin.galileo.usg.edu/dpla
    *
    * @param params URL parameters
    * @return
    */
  def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("https")
      .setHost("dlgadmin.galileo.usg.edu")
      .setPath("/dpla/")
      .setParameter("cursormark", params.getOrElse("cursorMark", "*")) // required
      .setParameter("rows", params.getOrElse("rows", "500")) // optional, default row value for API responses is 1000
      .build()
      .toURL
}
