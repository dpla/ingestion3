package dpla.ingestion3.harvesters.resourceSync

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection

/** This class interprets the user-submitted params to determine which type of
  * request to send to the ResourceSync feed (ie. get all records or a change
  * set, get those records by iterating over resourcelist or requesting TAR
  * files via dump). It requests a ResourceSync response via the
  * `RsResponseBuilder`. It constructs a DataFrame from the RS response.
  */
class RsRelation(
    endpoint: String,
    harvestAll: Boolean = true,
    capability: String = "resourcelist"
)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with Serializable {

  val rsResponseBuilder = new RsResponseBuilder(endpoint)(sqlContext)

  /** Make appropriate call to RsResponseBuilder
    */
  def records: RDD[RsResponse] = {
    (capability, harvestAll) match {
      case ("resourcelist", true)  => rsResponseBuilder.getAllResourceList()
      case ("resourcelist", false) => ???
      case ("resourcedump", true)  => ???
      case ("resoucedump", false)  => ???
      // Resources can be described by as sitemaps, see CO-WY
      case ("sitemaps", true) => ???
      case _ =>
        val msg = s"Capability $capability not   supported."
        throw new IllegalArgumentException(msg)
    }
  }

  // Get responses according to the verb.
  def getResponses: RDD[RsResponse] = {
    records
  }

  /** Set the schema for the DataFrame that will be returned on load. Columns:
    *   - record: RsRecord
    *   - error: String
    * Each row will have a non-null value for ONE of record or error.
    */
  override def schema: StructType = {
    val recordType = ScalaReflection
      .schemaFor[RsRecord]
      .dataType
      .asInstanceOf[StructType]
    val errorType = ScalaReflection
      .schemaFor[RsError]
      .dataType
      .asInstanceOf[StructType]

    val recordField = StructField("record", recordType, true)
    val errorField = StructField("error", errorType, true)
    StructType(Seq(recordField, errorField))
  }

  /** Build the rows for the DataFrame.
    */
  override def buildScan(): RDD[Row] = {

    getResponses.flatMap {
      case x: RsRecord => Seq(Row(x, None))
      case x: RsError  => Seq(Row(None, x))
      case x: RsSource => {
        val msg = "Unexpected RsSource type encountered when building DataFrame"
        val exception = new RsHarvesterException(msg)
        val error = RsError(exception.toString, x)
        Seq(Row(None, error))
      }
    }
  }
}
