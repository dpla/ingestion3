
package dpla.ingestion3.harvesters.api

import java.net.URL

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.mappers.utils.JsonExtractor
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success}

/**
  * Class for harvesting records from Primo endpoints
  *
  * The buildUrl(queryParams: Map[String, String] method is not
  * defined here but should instead be defined in a specific
  * provider's implementation of this abstract class.
  *
  */
abstract class PrimoApiHarvester(spark: SparkSession,
                                 shortName: String,
                                 conf: i3Conf,
                                 logger: Logger)
  extends ApiHarvester(spark, shortName, conf, logger) with JsonExtractor {

  def mimeType: String = "application_json"

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query,
    "rows" -> conf.harvest.rows,
    "apiKey" -> conf.harvest.apiKey,
    "offset" -> Some("0")
  ).collect{ case (key, Some(value)) => key -> value } // remove None values

  /**
    * Entry point for running the harvest
    *
    * @return DataFrame of harvested records
    */
  override def localHarvest(): DataFrame = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var offset = "0" // record offset, deliberate misspelling to match Primo naming for this parameter
    var totalRecords = "" // total number of records to fetch

    while(continueHarvest) getSinglePage(offset) match {
      // Handle errors
      case error: ApiError with ApiResponse =>
        logger.error("Error returned by request %s\n%s\n%s".format(
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
            val primoRecords = (json \ "docs").children.flatMap(doc => {
              extractStrings(doc \ "pnx" \ "search" \ "recordid").headOption match {
                case Some(id) => Some(ApiRecord(id, Utils.formatJson(doc)))
                case _ => None // drop records w/o ID, should raise error but /eheu
              }
            })

            // @see ApiHarvester
            saveOutRecords(primoRecords)

            println(primoRecords.size)

            // Loop control
            val nextOffset = (primoRecords.size + offset.toInt).toString
            // Only extract total records once
            totalRecords = if (totalRecords.isEmpty)
              (json \ "info" \ "total").extract[String]
            else totalRecords

            // Fetched 8,300 of 991,692 from http://mwdl.com/PrimoWebServices/xservice/search/brief?indx=8201?...
            logger.info(s"Fetched ${Utils.formatNumber(nextOffset.toLong)} " +
              s"of ${Utils.formatNumber(totalRecords.toLong)} " +
              s"from ${src.url.getOrElse("No url")}")

            if (offset.toInt >= totalRecords.toInt) {
              continueHarvest = false
            } else offset = nextOffset
          case _ =>
            logger.error(s"Response body is empty.\n" +
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
    * Get a single-page, un-parsed response from a Primo endpoint, or an error if
    * one occurs.
    *
    * @param offset Record offset
    * @return ApiSource or ApiError
    */
  private def getSinglePage(offset: String): ApiResponse = {
    val url = buildUrl(queryParams.updated("offset", offset))

    HttpUtils.makeGetRequest(url) match {
      case Failure(e) =>
        ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
      case Success(response) => response.isEmpty match {
        case true => ApiError("Response body is empty", ApiSource(queryParams, Some(url.toString)))
        case false => ApiSource(queryParams, Some(url.toString), Some(response))
      }
    }
  }

  /**
    * Constructs the URL for Primo API requests, should be
    * defined in provider implementation of PrimoHarvester
    *
    * @param params Map[String, String] URL parameters
    * @return URL
    */
  def buildUrl(params: Map[String, String]): URL
}
