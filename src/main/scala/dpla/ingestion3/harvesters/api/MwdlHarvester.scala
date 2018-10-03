
package dpla.ingestion3.harvesters.api

import java.net.URL

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats

import scala.util.{Failure, Success}
import scala.xml.XML

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
      .setParameter("indx", params.getOrElse("indx", "1")) // pagination value
      .setParameter("bulkSize", params.getOrElse("rows", "10"))
      .setParameter("institution", "MWDL")
      .setParameter("loc", "local,scope:(mw)")
      .setParameter("query", params.getOrElse("query", throw new RuntimeException("No query parameter provided")))
      .setParameter("query_exec", "facet_rtype,exact,collections")
      .setParameter("query_exec", "facet_scope,exact,dd")
      .build()
      .toURL
}
