package dpla.ingestion3.harvesters.oai

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import scala.xml.XML

/*
 * This class interprets the user-submitted params to determine which type of
 * request to send to the OAI feed (ie. get records or sets of records).
 * It requests an OAI response via the `OaiResponseBuilder`.
 * It constructs a DataFrame from the OAI response.
  */
class OaiRelation (parameters: Map[String, String])
                  (@transient val sqlContext: SQLContext)
                  extends BaseRelation with TableScan with Serializable {

  // Required properties for all OAI requests.
  assume(parameters.get("path").isDefined)
  assume(parameters.get("verb").isDefined)

  val endpoint = parameters("path")
  val verb = parameters("verb")

  // Optional properties.
  val metadataPrefix: Option[String] = parameters.get("metadataPrefix")
  val sets: Option[String] = parameters.get("sets")

  val oaiResponseBuilder = new OaiResponseBuilder(sqlContext)

  // Get the appropriate response based on the value of the OAI verb.
  def response: RDD[String] = {
    verb match {
      case "ListSets" => setsResponse
      case "ListRecords" => recordsResponse
    }
  }

  def setsResponse: RDD[String] = {
    val oaiParams =  Map("endpoint" -> endpoint, "verb" -> verb)
    oaiResponseBuilder.getResponse(oaiParams)
  }

  /*
   * Make appropriate call to OaiResponseBuilder based on presence or absence of
   * sets.
   */
  def recordsResponse: RDD[String] = {
    // A ListRecords OAI request must have a metadataPrefix.
    assume(metadataPrefix.isDefined)

    val prefix = if (metadataPrefix.isDefined) metadataPrefix.get else ""

    val oaiParams = Map("endpoint" -> endpoint,
                        "verb" -> verb,
                        "metadataPrefix" -> prefix)

    sets match {
      case None => oaiResponseBuilder.getResponse(oaiParams)
      case Some(sets) => {
        val setArray: Array[String] = parseSets(sets)
        oaiResponseBuilder.getResponseBySets(oaiParams, setArray)
      }
    }
  }

  /*
   * Sets are passed to this class as a comma-separated String.
   * This parses the String to an Array.
   */
  def parseSets(string: String): Array[String] = {
    string.split(",").map(_.trim)
  }

  // Set the schema for the DataFrame that will be returned on load.
  override def schema: StructType = {
    StructType(Seq(StructField("id", StringType, true),
                   StructField("document", StringType, true)))
  }

  // Build the rows for the DataFrame.
  override def buildScan(): RDD[Row] = {
    response.flatMap(
      // page [String] one page of records
      page => {
        val xml = XML.loadString(page)
        OaiResponseProcessor.getItems(xml, verb).map {
          case (id, item) => Row(id, item)
        }
      }
    )
  }
}
