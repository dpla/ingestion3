package dpla.ingestion3.harvesters.api

import java.net.URL
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.model.AVRO_MIME_JSON
import dpla.ingestion3.utils.HttpUtils
import org.apache.avro.generic.GenericData
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.util.{Failure, Success}

class MdlHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends ApiHarvester(spark, shortName, conf) {

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_JSON

  protected val defaultRows = "50"
  protected val defaultQuery = "*:*"

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query.getOrElse(defaultQuery),
    "rows" -> conf.harvest.rows.getOrElse(defaultRows)
  )

  override def localHarvest(): DataFrame = {

    val logger = LogManager.getLogger(this.getClass)

    implicit val formats: DefaultFormats.type = DefaultFormats

    // Mutable var for controlling harvest loop
    var requestUrl: Option[String] = Some(getFirstUrl(queryParams))

    while (requestUrl.isDefined) requestUrl match {
      // do nothing, there is no url to request
      case None => None

      // next url to request is defined
      case Some(url) =>
        getSinglePage(url) match {

          // Case 1 - handle valid response from api
          case src: ApiSource with ApiResponse =>
            src.text match {
              case Some(docs) =>
                val json = parse(docs)
                // Get next page to request. If there is no next page to request the 'next' property will not exist
                requestUrl = (json \ "links" \ "next").extractOpt[String]

                logger.info(
                  s"Next page to request -- ${requestUrl.getOrElse("No next URL")}"
                )

                // Extract records from response
                val mdlRecords = (json \ "data").children.map(
                  doc => { // documents field has changed with v2 of API
                    ApiRecord(
                      (doc \ "id").toString,
                      compact(render(doc))
                    ) // ID field has changed with v2 of API
                  }
                )

                saveOutRecords(mdlRecords)
              // valid response (e.g. http 200) from api but empty body
              case None =>
                logger.error(
                  s"The body of the response is empty. Stopping run.\nApiSource >> ${src.toString}"
                )
                requestUrl = None // stop the harvest
            }

          // Case 2 - error returned by requesting page
          case error: ApiError with ApiResponse =>
            logger.error(
              "Error returned by request %s\n%s\n%s".format(
                error.errorSource.url.getOrElse("Undefined url"),
                error.errorSource.queryParams,
                error.message
              )
            )
            requestUrl = None // stop the harvest

          // Case 3 - unknown
          case _ =>
            logger.error("Harvest returned None")
            requestUrl = None // stop the harvest
        }
    }

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }

  /** Get a single-page, un-parsed response from the API feed, or an error if
    * one occurs.
    *
    * @param urlString
    *   URL to fetch
    * @return
    *   ApiSource or ApiError
    */
  private def getSinglePage(urlString: String): ApiResponse = {
    val url = new URL(urlString)
    HttpUtils.makeGetRequest(url) match {
      case Failure(e) =>
        ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
      case Success(response) if response.isEmpty =>
        ApiError(
          "Response body is empty",
          ApiSource(queryParams, Some(url.toString))
        )
      case Success(response) =>
        ApiSource(queryParams, Some(url.toString), Some(response))
    }
  }

  /** First MDL URL from parameters
    *
    * Base URL https://lib-metl-prd-01.oit.umn.edu/api/v2/records
    *
    * @param queryParams
    *   URL parameters
    * @return
    *   URI
    */
  def getFirstUrl(queryParams: Map[String, String]): String = {
    val url = new URIBuilder()
      .setScheme("https")
      .setHost("lib-metl-prd-01.oit.umn.edu")
      .setPath("/api/v2/records")
      .setParameter("page[size]", queryParams.getOrElse("rows", defaultRows))

    /** This is a real mangling. The configuration file does not really support
      * querying multiple fields so we need to split them apart (.split(, )) and
      * build a set of key/value pairs to use when adding parameters to the
      * URIBuilder object
      */
    val params = queryParams
      .getOrElse("query", defaultQuery)
      .split(", ")
      .map(query => {
        val query_tuple = query.split("=")
        Parameters(query_tuple(0), query_tuple(1))
      })

    // Add query parameters to URL
    params.foreach(p => url.addParameter(p.name, p.value))

    url.build().toURL.toString
  }
}

case class Parameters(name: String, value: String)
