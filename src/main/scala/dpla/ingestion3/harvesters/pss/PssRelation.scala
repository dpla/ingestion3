package dpla.ingestion3.harvesters.pss

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

/*
 * This class requests a PSS response via the `PssResponseBuilder`.
 * It constructs a DataFrame from the response.
 */
class PssRelation(parameters: Map[String, String])(
    @transient val sqlContext: SQLContext
) extends BaseRelation
    with TableScan {

  // Required properties.
  assume(parameters.get("path").isDefined)

  val endpoint = parameters("path")

  /*
   * Get partitioned PSS data.
   * The first value of the response tuple is the set ID (ie. slug).
   * The second value is the full text of the pss metadata record.
   */
  def sets: RDD[(String, String)] = {
    val pssResponseBuilder = new PssResponseBuilder(sqlContext)
    pssResponseBuilder.getSets(endpoint)
  }

  // Set the schema for the DataFrame that will be returned on load.
  override def schema: StructType = {
    StructType(
      Seq(
        StructField("id", StringType, true),
        StructField("document", StringType, true)
      )
    )
  }

  // Build the rows for the DataFrame.
  override def buildScan(): RDD[Row] = {
    sets.map { case (id, set) => Row(id, set) }
  }
}
