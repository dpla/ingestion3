package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.harvesters.OaiQueryUrlBuilder
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import java.net.URL
import scala.xml.{NodeSeq, XML}

class OaiRelation (endpoint: String, metadataPrefix: String, verb: String)
                  (@transient val sqlContext: SQLContext)
                  extends BaseRelation with TableScan {

  val oaiParams = Map[String,String](
    "endpoint" -> endpoint,
    "metadataPrefix" -> metadataPrefix,
    "verb" -> verb)
  
  override def schema: StructType =  {
    StructType(Seq(StructField("id", StringType, true), 
                   StructField("document", StringType, true)))
  }

  /*
  * Each Row contains two Strings. The first is the document ID and the second
  * is the XML text of the record.
  */
  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext.parallelize(results.map(x => Row(x._1, x._2)))
  }

  def results: Seq[(String, String)] = {
    val urlBuilder = new OaiQueryUrlBuilder
    val url = urlBuilder.buildQueryUrl(oaiParams)
    val xml = getXmlResponse(url)
    val oaiProcessor = new OaiResponseProcessor
    oaiProcessor.getRecordsAsMap(xml)
  }

  /**
  * Executes the request and returns the response
  *
  * @param url URL
  *            OAI request URL
  * @return NodeSeq
  *         XML response
  */
  def getXmlResponse(url: URL): NodeSeq = {
    XML.load(url)
  }
}
