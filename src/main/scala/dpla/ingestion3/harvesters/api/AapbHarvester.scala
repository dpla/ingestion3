package dpla.ingestion3.harvesters.api

import java.net.URL
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.model.AVRO_MIME_XML
import dpla.ingestion3.utils.HttpUtils
import org.apache.avro.generic.GenericData
import org.apache.http.client.utils.URIBuilder
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonAST.{JArray, JString, JValue}
import org.json4s.jackson.JsonMethods.parse

import scala.util.{Failure, Success, Try}

/** TEST HUB — see docs/ingestion/README_TEST_HUBS.md and
  * docs/ingestion/aapb-mapping-draft.md.
  *
  * Harvester for the American Archive of Public Broadcasting (AAPB). AAPB's
  * OAI-PMH endpoint is non-functional, so this harvests from AAPB's Solr API
  * (`https://americanarchive.org/api.json`), pulling the full PBCore document for
  * each record inline via `fl=id,xml`.
  *
  * Enumeration uses Solr **`cursorMark`** paging (not `start`/offset): the first
  * request sends `cursorMark=*`, each response returns a `nextCursorMark`, and the
  * harvest stops when the mark stops changing. This is cap-free and stable across
  * the whole corpus (offset paging risks a `maxWindowSize` ceiling). `cursorMark`
  * requires a deterministic sort with a unique tiebreaker, so we sort by `id asc`.
  *
  * Scope is controlled by `aapb.harvest.setlist` (comma-separated `access_types`
  * values, OR'd into an `fq`). Default = the digitized ∪ online ∪ on-location
  * superset (~348k records); `access_types:all` (~2.7M) is avoided as it is mostly
  * inventory-only metadata stubs with no digital object.
  */
class AapbHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends ApiHarvester(shortName, conf) {

  import AapbHarvester._

  private val logger = LogManager.getLogger(this.getClass)

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML // the document is PBCore XML

  private val endpoint: String = conf.harvest.endpoint.getOrElse(DefaultEndpoint)

  // access_types to include (OR'd into the Solr fq). From aapb.harvest.setlist,
  // else the digitized/online/on-location superset.
  private val accessTypes: Seq[String] =
    conf.harvest.setlist
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .filter(_.nonEmpty)
      .getOrElse(DefaultAccessTypes)

  private val filterQuery: String = accessFilter(accessTypes)
  private val query: String = conf.harvest.query.getOrElse("*")
  private val rows: String = conf.harvest.rows.getOrElse(DefaultRows)

  override protected val queryParams: Map[String, String] =
    Map("q" -> query, "rows" -> rows)

  override def harvest: DataFrame = {
    implicit val formats: Formats = DefaultFormats

    var continueHarvest = true
    var cursorMark = "*"

    while (continueHarvest) getSinglePage(cursorMark) match {
      case error: ApiError with ApiResponse =>
        logger.error(
          "Error returned by request %s\n%s\n%s".format(
            error.errorSource.url.getOrElse("Undefined url"),
            error.errorSource.queryParams,
            error.message
          )
        )
        continueHarvest = false

      case src: ApiSource with ApiResponse =>
        src.text match {
          case Some(body) =>
            Try(parse(body)) match {
              case Success(json) =>
                val (records, nextCursorMark) = parsePage(json)
                saveOutRecords(records)
                logger.info(s"Harvested ${records.size} records (cursorMark=$cursorMark)")

                // Solr signals the end by returning the same cursorMark it was sent.
                if (records.isEmpty || nextCursorMark.isEmpty || nextCursorMark == cursorMark)
                  continueHarvest = false
                else
                  cursorMark = nextCursorMark

              case Failure(f) =>
                logger.error(
                  s"Unable to parse response\nURL: ${src.url.getOrElse("Not set")}\n" +
                    s"Params: ${src.queryParams}\nError: ${f.getMessage}"
                )
                continueHarvest = false // stop rather than re-request the same page
            }
          case _ =>
            logger.error(
              s"Response body is empty.\nURL: ${src.url.getOrElse("Not set")}\n" +
                s"Params: ${src.queryParams}"
            )
            continueHarvest = false
        }

      case _ => throw new RuntimeException("Unexpected ApiResponse type")
    }

    spark.read.format("avro").load(tmpOutStr)
  }

  private def getSinglePage(cursorMark: String): ApiResponse = {
    val url = buildUrl(
      endpoint = endpoint,
      query = query,
      rows = rows,
      filterQuery = filterQuery,
      cursorMark = cursorMark
    )
    logger.info(s"Requesting ${url.toString}")

    Try(HttpUtils.makeGetRequest(url)) match {
      case Failure(e) =>
        ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
      case Success(response) if response.isEmpty =>
        ApiError("Response body is empty", ApiSource(queryParams, Some(url.toString)))
      case Success(response) =>
        ApiSource(queryParams, Some(url.toString), Some(response))
    }
  }
}

object AapbHarvester {

  val DefaultEndpoint = "https://americanarchive.org/api.json"
  val DefaultRows = "500"
  // digitized ∪ online ∪ on-location — every catalog record that has a digital
  // object (excludes ~2.3M inventory-only stubs carried under access_types:all).
  val DefaultAccessTypes: Seq[String] = Seq("digitized", "online", "on-location")

  /** OR the requested access_types into a single Solr filter query. */
  def accessFilter(types: Seq[String]): String =
    types.map(t => s"access_types:$t").mkString(" OR ")

  /** Build one Solr API request URL. `fl=id,xml` returns the PBCore inline; the
    * deterministic `sort=id asc` is required for cursorMark paging.
    */
  def buildUrl(
      endpoint: String,
      query: String,
      rows: String,
      filterQuery: String,
      cursorMark: String
  ): URL =
    new URIBuilder(endpoint)
      .setParameter("q", query)
      .setParameter("fq", filterQuery)
      .setParameter("fl", "id,xml")
      .setParameter("rows", rows)
      .setParameter("sort", "id asc")
      .setParameter("cursorMark", cursorMark)
      .build()
      .toURL

  /** Extract (records, nextCursorMark) from one parsed Solr JSON response. Each
    * doc contributes an ApiRecord(id, pbcoreXml); the `xml` field may be a JSON
    * string or a single-element array.
    */
  def parsePage(json: JValue)(implicit formats: Formats): (List[ApiRecord], String) = {
    val nextCursorMark = (json \ "nextCursorMark").extractOrElse[String]("")

    val records = (json \ "response" \ "docs").children.flatMap { doc =>
      val id = (doc \ "id").extractOpt[String].map(_.trim).filter(_.nonEmpty)
      val xml = firstString(doc \ "xml").map(_.trim).filter(_.nonEmpty)
      for { i <- id; x <- xml } yield ApiRecord(i, x)
    }

    (records, nextCursorMark)
  }

  private def firstString(value: JValue): Option[String] = value match {
    case JString(s)    => Some(s)
    case JArray(items) => items.collectFirst { case JString(s) => s }
    case _             => None
  }
}
