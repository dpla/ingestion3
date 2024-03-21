package dpla.ingestion3.harvesters.pss

import java.net.URL

import dpla.ingestion3.utils.HttpUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.util.{Failure, Success}

/*
 * This class handles requests to the PSS feed.
 * It partitions data at strategic points.
 */
class PssResponseBuilder(@transient val sqlContext: SQLContext)
    extends Serializable {

  // Get all sets.
  def getSets(endpoint: String): RDD[(String, String)] = {
    // The given endpoint returns a list of all set URLs.
    val url = new URL(endpoint)
    val allSets = getStringResponse(url)
    val setEndpoints = PssResponseProcessor.getSetEndpoints(allSets)
    val setEndpointsRdd = sqlContext.sparkContext.parallelize(setEndpoints)
    setEndpointsRdd.map(setEndpoint => getSet(setEndpoint))
  }

  // Get a single set.
  def getSet(endpoint: String): (String, String) = {
    // The given endpoint contains metadata for the source, along with a
    // list of URLs for component parts of the set.
    val setUrl = new URL(endpoint)
    val setId = PssResponseProcessor.getSetId(endpoint)
    val set = getStringResponse(setUrl)
    val parts = getParts(set)
    val setWithParts = PssResponseProcessor.combineSetAndParts(set, parts)
    (setId, setWithParts)
  }

  // Get all component parts of a set (ie. sources and teaching guides).
  def getParts(set: String): List[String] = {
    // The given endpoints contain metadata for component parts of a set.
    val endpoints = PssResponseProcessor.getPartEndpoints(set)
    endpoints.map(endpoint => {
      val url = new URL(endpoint)
      getStringResponse(url)
    })
  }

  /** Executes the request and returns the response
    *
    * @param url
    *   URL PSS request URL
    * @return
    *   String String response
    */
  def getStringResponse(url: URL): String = {
    HttpUtils.makeGetRequest(url) match {
      case Success(s) => s
      // TODO: Handle failed HTTP request.
      case Failure(f) => throw f
    }
  }
}
