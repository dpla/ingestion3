package dpla.ingestion3.harvesters.oai.refactor

import dpla.ingestion3.harvesters.oai.{OaiError, OaiRecord, OaiSet}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}

abstract class OaiRelation extends BaseRelation with TableScan with Serializable {
  /**
    * Set the schema for the DataFrame that will be returned on load.
    * Columns:
    *   - set: OaiSet
    *   - record: OaiRecord
    *   - error: String
    * Each row will have a non-null value for ONE of sets, records, or error.
    */
  override def schema: StructType = {
    val setType = getStruct[OaiSet]
    val recordType = getStruct[OaiRecord]
    val errorType = getStruct[OaiError]
    val setField = StructField("set", setType, nullable = true)
    val recordField = StructField("record", recordType, nullable = true)
    val errorField = StructField("error", errorType, nullable = true)
    StructType(Seq(setField, recordField, errorField))
  }

  private def getStruct[T] =
    ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  override def buildScan(): RDD[Row]
}
