
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
class MsHarvester(spark: SparkSession,
                  shortName: String,
                  conf: i3Conf,
                  harvestLogger: Logger)
  extends PrimoVEHarvester(spark, shortName, conf, harvestLogger) {

  /**
    * Constructs the URL for Mississippi Primo VE API requests
    *
    * @param params URL parameters
    * @return URL
    */

//  https://api-na.hosted.exlibrisgroup.com/primo/v1/search
//  ?vid=01USM_INST:MDL
//  &tab=MDL
//  &scope=MDL
//  &q=any,contains,**
//  &apikey=
//  &indx=0
  override def buildUrl(params: Map[String, String]): URL =
    new URIBuilder()
      .setScheme("https")
      .setHost("api-na.hosted.exlibrisgroup.com")
      .setPath("/primo/v1/search")
      .setParameter("indx", params.getOrElse("indx", "1")) // record offset
      .setParameter("vid", "01USM_INST:MDL")
      .setParameter("tab", "MDL")
      .setParameter("scope", "MDL")
      .setParameter("q", params.getOrElse("query", throw new RuntimeException("No query parameter provided")))
      .setParameter("apikey", params.getOrElse("api_key", throw new RuntimeException("No API key provided")))
      .build()
      .toURL
}
