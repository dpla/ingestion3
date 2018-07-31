
package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success}

class MdlHarvester(spark: SparkSession,
                   shortName: String,
                   conf: i3Conf,
                   outputDir: String,
                   harvestLogger: Logger)
  extends ApiHarvester(spark, shortName, conf, outputDir, harvestLogger) {

  override protected val mimeType: String = "application_json"

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query.getOrElse("*:*"),
    "rows" -> conf.harvest.rows.getOrElse("10")
  )

  override protected def localHarvest: Unit = {
    implicit val formats = DefaultFormats

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var start = 0

    while (continueHarvest) getSinglePage(start.toString) match {
      case error: ApiError with ApiResponse =>
        harvestLogger.error("Error returned by request %s\n%s\n%s".format(
          error.errorSource.url.getOrElse("Undefined url"),
          error.errorSource.queryParams,
          error.message
        ))
        continueHarvest = false
      case src: ApiSource with ApiResponse =>
        src.text match {
          case Some(docs) =>
            val json = parse(docs)

            harvestLogger.info(s"Requesting $start of ${(json \\ "numFound").extract[String]}")

            val mdlRecords = (json \\ "docs").children.map(doc => {
              ApiRecord((doc \\ "record_id").toString, compact(render(doc)))
            })

            saveOutRecords(mdlRecords)

            // Number of records returned < number of records requested
            val rows = queryParams.getOrElse("rows", "10").toInt
            mdlRecords.size < rows match {
              case true => continueHarvest = false
              case false => start += rows
            }
          case None =>
            harvestLogger.error(s"The body of the response is empty. Stopping run.\nApiSource >> ${src.toString}")
            continueHarvest = false
        }
      case _ =>
        harvestLogger.error("Harvest returned None")
        continueHarvest = false
    }
  }


    /**
    * Get a single-page, un-parsed response from the API feed, or an error if
    * one occurs.
    *
    * @param start Uses start as an offset to paginate.
    * @return ApiSource or ApiError
    */
  private def getSinglePage(start: String): ApiResponse = {
    val url = buildUrl(queryParams.updated("start", start))
    HttpUtils.makeGetRequest(url) match {
      case Failure(e) =>
        ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
      case Success(response) if response.isEmpty =>
          ApiError("Response body is empty", ApiSource(queryParams, Some(url.toString)))
      case Success(response) =>
        ApiSource(queryParams, Some(url.toString), Some(response))
    }
  }

  /**
    * Builds MDL query URL from parameters
    *
    * @param queryParams URL parameters
    * @return URI
    */
  def buildUrl(queryParams: Map[String, String]): URL = new URIBuilder()
    .setScheme("http")
    .setHost("hub-client.lib.umn.edu")
    .setPath("/api/v1/records")
    .setParameter("q", queryParams.getOrElse("query", "*:*"))
    .setParameter("start", queryParams.getOrElse("start", "0"))
    .setParameter("rows", queryParams.getOrElse("rows", "10"))
    .build()
    .toURL
}
