package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/** Offset-paging harvester for the Getty Primo VE endpoint.
  *
  * RETAINED BUT NOT IN USE. Getty's Primo gateway caps any single query at
  * `offset <= 1999` and `limit <= 1000`, so paging stops at ~2,000 of ~101,400
  * records -- and reports SUCCESS, which is how a 98% shortfall reached
  * production in February 2026. [[GettyProfile]] points at
  * [[GettyRefreshHarvester]] instead.
  *
  * This class is kept deliberately: it is the correct implementation the moment
  * Ex Libris lifts the offset cap, which is the outcome we are pressing Getty
  * for. Re-point [[GettyProfile]] here if that happens -- and verify against
  * `info.total` (~101,400) before trusting the result.
  */
class GettyHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends PrimoVEHarvester(spark, shortName, conf) {

  /** Constructs the URL for Getty Primo VE API requests
    *
    * @param params
    *   URL parameters
    * @return
    *   URL
    */
  override def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("https")
      .setHost("api-na.hosted.exlibrisgroup.com")
      .setPath("/primo/v1/search")
      .setParameter("offset", params.getOrElse("offset", "0")) // record offset
      .setParameter("vid", "DPLA")
      .setParameter("tab", "dpla")
      .setParameter("scope", "DPLA")
      .setParameter("inst", "01GRI")
      .setParameter(
        "q",
        params.getOrElse(
          "query",
          throw new RuntimeException("No query parameter provided")
        )
      )
      .setParameter(
        "apikey",
        params.getOrElse(
          "api_key",
          throw new RuntimeException("No API key provided")
        )
      )
      .setParameter("lang", "eng")
      .setParameter("limit", "500")
      .build()
      .toURL
}
