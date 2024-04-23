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
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.util.{Failure, Success, Try}

class IaHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends ApiHarvester(spark, shortName, conf) {

  //
  def mimeType: GenericData.EnumSymbol = AVRO_MIME_JSON

  /** */
  override protected val queryParams: Map[String, String] = Map(
    "q" -> conf.harvest.query
  ).collect { case (key, Some(value)) => key -> value } // remove None values

  /** */
  override def localHarvest(): DataFrame = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val iaCollections = conf.harvest.setlist.getOrElse("").split(",")

    val logger = LogManager.getLogger(this.getClass)

    iaCollections.foreach(collection => {
      // Mutable vars for controlling harvest loop
      var continueHarvest = true
      var cursor = ""

      queryParams.updated("q", s"collection:$collection")

      while (continueHarvest) getSinglePage(cursor, collection) match {
        // Handle errors
        case error: ApiError with ApiResponse =>
          logger.error(
            "Error returned by request %s\n%s\n%s".format(
              error.errorSource.url.getOrElse("Undefined url"),
              error.errorSource.queryParams,
              error.message
            )
          )
          continueHarvest = false
        // Handle a successful response
        case src: ApiSource with ApiResponse =>
          src.text match {
            case Some(docs) =>
              Try { parse(docs) } match {
                case Success(json: JValue) => {
                  val iaRecords = (json \\ "items").children
                    .map(doc => {
                      val identifier = (doc \\ "identifier").toString
                      if (identifier.nonEmpty)
                        ApiRecord(identifier, compact(render(doc)))
                      else
                        logger.error(s"""No identifier in original record
                             |URL: ${src.url.getOrElse("Not set")}
                             |Params: ${src.queryParams}
                             |Body: $doc
                             |""".stripMargin)
                    })
                    .collect { case a: ApiRecord => a }

                  // @see ApiHarvester
                  saveOutRecords(iaRecords)

                  // Loop control
                  cursor = (json \\ "cursor").extractOrElse[String]("")

                  if (cursor.isEmpty)
                    continueHarvest = false
                }
                case Failure(f) =>
                  logger.error(
                    s"Unable to parse response\n" +
                      s"URL: ${src.url.getOrElse("Not set")}\n" +
                      s"Params: ${src.queryParams}\n" +
                      s"Body: $docs\n" +
                      s"Error: ${f.getMessage}"
                  )
              }
            // Handle unknown case
            case _ =>
              logger.error(
                s"Response body is empty.\n" +
                  s"URL: ${src.url.getOrElse("!!! URL not set !!!")}\n" +
                  s"Params: ${src.queryParams}\n" +
                  s"Body: ${src.text}"
              )
              continueHarvest = false
          }
        case _ => throw new RuntimeException("Not sure how we got here!")
      }
    })

    spark.read.format("avro").load(tmpOutStr)
  }

  /** Get a single-page, un-parsed response from the IA Scrape API, or an error
    * if one occurs.
    *
    * @param cursor
    *   Uses cursor and not start/offset to paginate. Used to work around Solr
    *   deep-paging performance issues.
    * @return
    *   ApiSource or ApiError
    */
  private def getSinglePage(cursor: String, collection: String): ApiResponse = {
    val url = buildUrl(
      queryParams
        .updated("cursor", cursor)
        .updated("q", s"collection:$collection")
        .filter { case (k: String, v: String) => v.nonEmpty }
    )

    LogManager.getLogger(this.getClass).info(s"Requesting ${url.toString}")

    HttpUtils.makeGetRequest(url) match {
      case Failure(e) =>
        ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
      case Success(response) =>
        if (response.isEmpty) {
          ApiError(
            "Response body is empty",
            ApiSource(queryParams, Some(url.toString))
          )
        } else {
          ApiSource(queryParams, Some(url.toString), Some(response))
        }
    }
  }

  /** Constructs the URL for IA Scrape API requests
    *
    * @param params
    *   URL parameters
    * @return
    */
  def buildUrl(params: Map[String, String]): URL = {
    val uriBuilder = new URIBuilder()
    uriBuilder
      .setScheme("https")
      .setHost("archive.org")
      .setPath("/services/search/v1/scrape")
      .setParameter("q", params.getOrElse("q", "*:*"))
      .setParameter(
        "fields",
        "collection,contributor,creator,date,description,identifier,language,licenseurl,mediatype,publisher,rights,subject,title,volume"
      )

    // A blank or empty cursor valid is not allowed
    if (params.get("cursor").isDefined)
      uriBuilder.setParameter("cursor", params.getOrElse("cursor", ""))

    uriBuilder.build().toURL
  }
}
