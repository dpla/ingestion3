
package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Class for harvesting records from the Getty Primo endpoint
  *
  */
class GettyHarvester(spark: SparkSession,
                     shortName: String,
                     conf: i3Conf,
                     harvestLogger: Logger)
  extends PrimoApiHarvester(spark, shortName, conf, harvestLogger) {

  /**
    * Constructs the URL for Getty Primo API requests
    *
    * @param params URL parameters
    * @return URL
    */
  override def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("https")
      .setHost("api-na.hosted.exlibrisgroup.com")
      .setPath("/primo/v1/search")
      .setParameter("offset", params.getOrElse("offset", "0")) // record offset
      .setParameter("limit", params.getOrElse("rows", "500")) // records per page
      .setParameter("inst", "01GRI")
      .setParameter("vid", "GRI")
      .setParameter("tab", "all_gri")
      .setParameter("scope", "COMBINED")
      .setParameter("lang", "eng")
      .setParameter("loc", "local,scope:(GETTY_OCP,GETTY_ROSETTA)")
      .setParameter("q", params.getOrElse("query", throw new RuntimeException("No query parameter provided")))
      .setParameter("apikey", params.getOrElse("apiKey", throw new RuntimeException("No api key provided")))
      .build()
      .toURL
}
