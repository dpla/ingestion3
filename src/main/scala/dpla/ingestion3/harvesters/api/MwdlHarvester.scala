
package dpla.ingestion3.harvesters.api

import java.net.URL

import dpla.ingestion3.confs.i3Conf
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Class for harvesting records from the MWDL Primo endpoint
  *
  */
class MwdlHarvester(spark: SparkSession,
                    shortName: String,
                    conf: i3Conf,
                    harvestLogger: Logger)
  extends PrimoHarvester(spark, shortName, conf, harvestLogger) {

  /**
    * Constructs the URL for MWDL Primo API requests
    *
    * @param params URL parameters
    * @return URL
    */
  override def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("http")
      .setHost("utah-primoprod.hosted.exlibrisgroup.com")
      .setPath("/PrimoWebServices/xservice/search/brief")
      .setParameter("indx", params.getOrElse("indx", "1")) // record offset
      .setParameter("bulkSize", params.getOrElse("rows", "10")) // records per page
      .setParameter("institution", "MWDL")
      .setParameter("loc", "local,scope:(mw)")
      .setParameter("query", params.getOrElse("query", throw new RuntimeException("No query parameter provided")))
      .setParameter("query_exec", "facet_rtype,exact,collections")
      .setParameter("query_exec", "facet_scope,exact,dd")
      .build()
      .toURL
}
