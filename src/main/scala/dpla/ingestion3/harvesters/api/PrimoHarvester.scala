package dpla.ingestion3.harvesters.api

import java.net.URL
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.model.AVRO_MIME_XML
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats

import scala.util.{Failure, Success, Try}
import scala.xml.XML

/** Class for harvesting records from Primo Classic Web Services endpoints
  *
  * The buildUrl(queryParams: Map[String, String] method is not defined here but
  * should instead be defined in a specific provider's implementation of this
  * abstract class.
  */
abstract class PrimoHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends ApiHarvester(shortName, conf) {

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  override protected val queryParams: Map[String, String] = Map(
    "query" -> conf.harvest.query,
    "rows" -> conf.harvest.rows,
    "indx" -> Some("1")
  ).collect { case (key, Some(value)) => key -> value } // remove None values

  /** Entry point for running the harvest
    *
    * @return
    *   DataFrame of harvested records
    */
  override def harvest: DataFrame = {

    val logger = LogManager.getLogger(this.getClass)
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var indx =
      "1" // record offset, deliberate misspelling to match Primo naming for this parameter
    var totalRecords = "" // total number of records to fetch

    while (continueHarvest) getSinglePage(indx) match {
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
            val xml = XML.loadString(docs)
            // FIXME add 'sear' namespace to selection
            val primoRecords =
              (xml \\ "SEGMENTS" \ "JAGROOT" \ "RESULT" \ "DOCSET" \ "DOC")
                .map(doc => {
                  val id =
                    (doc \\ "PrimoNMBib" \ "record" \ "control" \ "recordid").text
                  ApiRecord(id, Utils.formatXml(doc))
                })
                .toList

            // @see ApiHarvester
            saveOutRecords(primoRecords)

            // Loop control
            val indxInt = indx.toInt
            val nextIndx = (primoRecords.size + indxInt).toString
            // Only extract total records once
            totalRecords =
              if (totalRecords.isEmpty)
                (xml \\ "SEGMENTS" \ "JAGROOT" \ "RESULT" \ "DOCSET" \ "@TOTALHITS").text
              else totalRecords

            // Log endpoint + TOTALHITS on first page unconditionally (stderr
            // bypasses broken log4j2 config so this is always visible).
            if (indxInt == 1)
              System.err.println(
                s"$shortName: harvest starting — " +
                  s"TOTALHITS=${if (totalRecords.isEmpty) "(not in response)" else totalRecords}, " +
                  s"endpoint=${src.url.getOrElse("(url not available)")}"
              )

            if (primoRecords.isEmpty) {
              // Empty page means the API has no more results at this offset
              // (Primo has a deep pagination limit below TOTALHITS). Stop here
              // rather than looping forever on the same offset.
              val recordsFetched = indxInt - 1
              val xmlSnippet = docs.take(600).replaceAll("\\s+", " ").trim
              System.err.println(
                s"""$shortName: PAGINATION LIMIT HIT — harvest stopping early
                   |  Offset (indx):        $indx
                   |  TOTALHITS (reported): ${if (totalRecords.isEmpty) "(not in response)" else totalRecords}
                   |  Records fetched:      $recordsFetched
                   |  Failing request URL:  ${src.url.getOrElse("(url not available)")}
                   |  Response (first 600): $xmlSnippet""".stripMargin
              )
              logger.warn(
                s"Empty page at indx=$indx " +
                  s"(TOTALHITS=${if (totalRecords.isEmpty) "(not in response)" else totalRecords}) — " +
                  s"stopping harvest at pagination limit"
              )
              continueHarvest = false
            } else {
              val totalRecordsLong = scala.util.Try(totalRecords.toLong).toOption
              logger.info(
                s"Fetched ${Utils.formatNumber(nextIndx.toLong - 1)} " +
                  s"of ${totalRecordsLong.map(Utils.formatNumber).getOrElse("?")} " +
                  s"from ${src.url.getOrElse("(url not available)")}"
              )

              totalRecordsLong match {
                case Some(total) if indxInt.toLong >= total => continueHarvest = false
                case None =>
                  // TOTALHITS missing from response — stop safely rather than
                  // risk looping forever without a termination condition
                  System.err.println(
                    s"$shortName: TOTALHITS missing from response at indx=$indx — stopping harvest"
                  )
                  continueHarvest = false
                case _ => indx = nextIndx
              }
            }
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
    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }

  /** Get a single-page, un-parsed response from a Primo endpoint, or an error
    * if one occurs.
    *
    * @param indx
    *   Record offset
    * @return
    *   ApiSource or ApiError
    */
  private def getSinglePage(indx: String): ApiResponse = {
    val url = buildUrl(queryParams.updated("indx", indx))

    Try { HttpUtils.makeGetRequest(url) } match {
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

  /** Constructs the URL for Primo API requests, should be defined in provider
    * implementation of PrimoHarvester
    *
    * @param params
    *   Map[String, String] URL parameters
    * @return
    *   URL
    */
  def buildUrl(params: Map[String, String]): URL
}
