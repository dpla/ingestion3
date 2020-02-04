
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
  extends PrimoHarvester(spark, shortName, conf, harvestLogger) {

  /**
    * Constructs the URL for Getty Primo API requests
    *
    * @param params URL parameters
    * @return URL
    */
  override def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("http")
      .setHost("primo.getty.edu")
      .setPath("/PrimoWebServices/xservice/search/brief")
      .setParameter("indx", params.getOrElse("indx", "1")) // record offset
      .setParameter("bulkSize", params.getOrElse("rows", "500")) // records per page
      .setParameter("institution", "01GRI")
      .setParameter("loc", "local,scope:(GETTY_OCP,GETTY_ROSETTA)")
      .setParameter("query", params.getOrElse("query", throw new RuntimeException("No query parameter provided")))
      .build()
      .toURL
}
