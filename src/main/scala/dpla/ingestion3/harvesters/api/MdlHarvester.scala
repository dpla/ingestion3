
package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import com.databricks.spark.avro._

import scala.util.{Failure, Success}

class MdlHarvester(spark: SparkSession,
                   shortName: String,
                   conf: i3Conf,
                   harvestLogger: Logger)
  extends ApiHarvester(spark, shortName, conf, harvestLogger) {

  def mimeType: String = "application_json"

  protected val defaultRows = "50"
  protected val defaultQuery = "*:*"

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query.getOrElse(defaultQuery),
    "rows" -> conf.harvest.rows.getOrElse(defaultRows)
  )

  override def localHarvest: DataFrame = {
    implicit val formats = DefaultFormats

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var requestUrl = getFirstUrl(queryParams)

    while (continueHarvest) getSinglePage(requestUrl) match {
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

//         "links":{
//           "first":"https://lib-metl-prd-01.oit.umn.edu/api/v2/records?page%5Bnumber%5D=1&page%5Bsize%5D=50",
//           "next":"https://lib-metl-prd-01.oit.umn.edu/api/v2/records?page%5Bnumber%5D=2&page%5Bsize%5D=50",
//           "last":"https://lib-metl-prd-01.oit.umn.edu/api/v2/records?page%5Bnumber%5D=29635&page%5Bsize%5D=50"}
//          }

            requestUrl = (json \ "links" \ "next").extract[String]
            harvestLogger.info(s"Next page to request $requestUrl")

            val mdlRecords = (json \ "data").children.map(doc => { // documents field has changed with v2 of API
              ApiRecord((doc \ "id").toString, compact(render(doc))) // ID field has changed with v2 of API
            })

            saveOutRecords(mdlRecords)

            continueHarvest = requestUrl.nonEmpty

          case None =>
            harvestLogger.error(s"The body of the response is empty. Stopping run.\nApiSource >> ${src.toString}")
            continueHarvest = false
        }
      case _ =>
        harvestLogger.error("Harvest returned None")
        continueHarvest = false
    }

    // Read harvested data into Spark DataFrame and return.
    spark.read.avro(tmpOutStr)
  }


    /**
    * Get a single-page, un-parsed response from the API feed, or an error if
    * one occurs.
    *
    * @param urlString URL to fetch
    * @return ApiSource or ApiError
    */
  private def getSinglePage(urlString: String): ApiResponse = {
    val url = new URL(urlString)
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
    *
    * First MDL URL from parameters
    *
    * Base URL https://lib-metl-prd-01.oit.umn.edu/api/v2/records
    *
    * @param queryParams URL parameters
    * @return URI
    */
  def getFirstUrl(queryParams: Map[String, String]): String = {
    val url = new URIBuilder()
      .setScheme("https")
      .setHost("lib-metl-prd-01.oit.umn.edu")
      .setPath("/api/v2/records")
      .setParameter("page[size]", queryParams.getOrElse("rows", defaultRows))


    /**
      * This is a real mangling.
      * The configuration file does not really support querying multiple fields so
      * we need to split them apart (.split(, )) and build a set of key/value pairs
      * to use when adding parameters to the URIBuilder object
      */
    val params = queryParams
      .getOrElse("query", defaultQuery)
      .split(", ")
      .map( query => {
        val query_tuple = query.split("=")
        Parameters(query_tuple(0), query_tuple(1))
      })

    // Add query parameters to URL
    params.foreach(p => url.addParameter(p.name, p.value))

    url.build().toURL.toString
  }
}

case class Parameters(name: String, value: String)