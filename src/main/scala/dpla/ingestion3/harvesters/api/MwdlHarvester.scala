package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import org.apache.http.client.utils.URIBuilder
import org.apache.spark.sql.SparkSession

/** Harvester for MWDL Primo VE REST API.
  *
  * The actual harvest runs via Python prefix scripts (mwdl-prefix-explorer.py
  * + mwdl-harvest.py) which work around Primo VE's ~9,900 offset limit using
  * trie-based title-prefix decomposition. This Scala harvester is wired in for
  * completeness but ingest.sh runs with --skip-harvest.
  */
class MwdlHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends PrimoVEHarvester(spark, shortName, conf) {

  /** Constructs the URL for MWDL Primo VE REST API requests.
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
      .setParameter("offset", params.getOrElse("offset", "1"))
      .setParameter("limit", params.getOrElse("limit", "100"))
      .setParameter("vid", "01UTAH_INST:MWDL")
      .setParameter("tab", "LibraryCatalog")
      .setParameter("scope", "MWDL")
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
      .build()
      .toURL
}
