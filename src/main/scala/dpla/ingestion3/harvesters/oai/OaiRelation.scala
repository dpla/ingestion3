package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.harvesters.OaiQueryUrlBuilder
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import java.net.URL
import org.apache.commons.io.IOUtils
import scala.xml.{NodeSeq, XML}

class OaiRelation (endpoint: String, metadataPrefix: String, verb: String)
                  (@transient val sqlContext: SQLContext)
                  extends BaseRelation with TableScan {

  val oaiParams = Map[String,String](
    "endpoint" -> endpoint,
    "metadataPrefix" -> metadataPrefix,
    "verb" -> verb)

  val urlBuilder = new OaiQueryUrlBuilder
  
  override def schema: StructType =  {
    StructType(Seq(StructField("id", StringType, true), 
                   StructField("document", StringType, true)))
  }

  /*
  * Each Row contains two Strings. The first is the document ID and the second
  * is the XML text of the record.
  */
  override def buildScan(): RDD[Row] = {
    val resultsRdd = sqlContext.sparkContext.parallelize(results)
    resultsRdd.flatMap(
      string => {
        val xml = XML.loadString(string)
        OaiResponseProcessor.getRecordsAsTuples(xml).map(
          tuple => {
            Row(tuple._1, tuple._2)
          }
        )
      }
    )
  }

  /*
  * Get all pages of results from an OAI feed.
  * Makes an inital call to the feed to get the first page of results.
  * For this and all subsequent pages, calls the next page if a resumption token
  * is present.
  * Returns a single List of single-page responses as Strings.
  */
  def results: List[String] = {

    def loop(data: List[String], 
             resumptionToken: Option[String]): List[String] = {
      if(!(resumptionToken.isDefined)) return data
      val queryParams = paramsWithToken(resumptionToken.get)
      val nextResponse = singlePageResponse(queryParams)
      val nextToken = OaiResponseProcessor.getResumptionToken(nextResponse)
      loop(nextResponse :: data, nextToken)
    }

    val firstResponse = singlePageResponse(oaiParams)
    val firstToken = OaiResponseProcessor.getResumptionToken(firstResponse)
    loop(List(firstResponse), firstToken)
  }

  def paramsWithToken(resumptionToken: String): Map[String, String] = {
    oaiParams ++ Map("resumptionToken" -> resumptionToken)
  }

  // Returns a single page response as a single String
  def singlePageResponse(queryParams: Map[String,String]): String = {
    val url = urlBuilder.buildQueryUrl(queryParams)
    println(url)  // For testing purposes, can delete later
    getStringResponse(url)
  }

  /**
  * Executes the request and returns the response
  *
  * @param url URL
  *            OAI request URL
  * @return String
  *         String response
  *
  * OAI-PMH XML responses must be enncoded as UTF-8.
  * @see https://www.openarchives.org/OAI/openarchivesprotocol.html#XMLResponse
  *
  * TODO: Handle failed HTTP request.
  */
  def getStringResponse(url: URL) : String = {
    IOUtils.toString(url, "UTF-8")
  }
}
