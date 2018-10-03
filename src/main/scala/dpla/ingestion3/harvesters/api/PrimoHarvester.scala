
package dpla.ingestion3.harvesters.api

import java.net.URL

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats

import scala.util.{Failure, Success}
import scala.xml.XML

/**
  * Class for harvesting records from Primo endpoints
  *
  */
abstract class PrimoHarvester(spark: SparkSession,
                              shortName: String,
                              conf: i3Conf,
                              harvestLogger: Logger)
  extends ApiHarvester(spark, shortName, conf, harvestLogger) {

  def mimeType: String = "application_xml"

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query,
    "rows" -> conf.harvest.rows,
    "indx" -> Some("1")
  ).collect{ case (key, Some(value)) => key -> value } // remove None values

  override def localHarvest: DataFrame = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var indx = "1"
    var totalRecords = ""

    // Runtime tracking
    val startTime = System.currentTimeMillis()

    while(continueHarvest) getSinglePage(indx) match {
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
            val xml = XML.loadString(docs)
            // FIXME add sear namespace to selection
            val primoRecords = (xml \\ "SEGMENTS" \"JAGROOT" \ "RESULT" \ "DOCSET" \ "DOC")
              .map(doc => ApiRecord((doc \\ "PrimoNMBib" \ "record" \ "control" \ "recordid").toString, Utils.formatXml(doc)))
              .toList

            // @see ApiHarvester
            saveOutRecords(primoRecords)

            // Loop control
            val nextIndx = (primoRecords.size + indx.toInt).toString
            totalRecords = if (totalRecords.isEmpty)
              (xml \\ "SEGMENTS" \ "JAGROOT" \ "RESULT" \ "DOCSET" \ "@TOTALHITS").text
            else totalRecords

            harvestLogger.info(s"Fetched ${Utils.formatNumber(indx.toLong)} of ${Utils.formatNumber(totalRecords.toLong)}")

            if (indx.toInt >= totalRecords.toInt) {
              continueHarvest = false
            } else indx = nextIndx

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
    * Get a single-page, un-parsed response from the MWDL Primo feed, or an error if
    * one occurs.
    *
    * @param indx Record offset
    * @return ApiSource or ApiError
    */
  private def getSinglePage(indx: String): ApiResponse = {
    val url = buildUrl(queryParams.updated("indx", indx))

    harvestLogger.info(s"Requesting ${url.toString}")

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
    * @param params URL parameters
    * @return
    */
  def buildUrl(params: Map[String, String]): URL
}
