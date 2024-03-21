package dpla.ingestion3.harvesters.oai.refactor

import dpla.ingestion3.harvesters.oai.OaiHarvesterException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}

/** Abstract parent class for classes that each handle the different types of
  * OaiHarvests. Contains common code (i.e. the schema right now)
  */

abstract class OaiRelation
    extends BaseRelation
    with TableScan
    with Serializable {

  /** Set the schema for the DataFrame that will be returned on load. Columns:
    *   - set: OaiSet
    *   - record: OaiRecord
    *   - error: String
    * Each row will have a non-null value for ONE of sets, records, or error.
    */
  override def schema: StructType = {
    val setType =
      ScalaReflection.schemaFor[OaiSet].dataType.asInstanceOf[StructType]
    val recordType =
      ScalaReflection.schemaFor[OaiRecord].dataType.asInstanceOf[StructType]
    val errorType =
      ScalaReflection.schemaFor[OaiError].dataType.asInstanceOf[StructType]
    val setField = StructField("set", setType, nullable = true)
    val recordField = StructField("record", recordType, nullable = true)
    val errorField = StructField("error", errorType, nullable = true)
    StructType(Seq(setField, recordField, errorField))
  }

  override def buildScan(): RDD[Row]
}

object OaiRelation {

  def getRelation(
      oaiMethods: OaiMethods,
      oaiConfig: OaiConfiguration,
      sqlContext: SQLContext
  ): OaiRelation =
    (oaiConfig.setlist, oaiConfig.harvestAllSets, oaiConfig.blacklist) match {
      case (None, false, Some(blacklist)) =>
        new BlacklistOaiRelation(oaiConfig, oaiMethods)(sqlContext)
      case (Some(setList), false, None) =>
        new WhitelistOaiRelation(oaiConfig, oaiMethods)(sqlContext)
      case (None, false, None) =>
        new AllRecordsOaiRelation(oaiConfig, oaiMethods)(sqlContext)
      case (None, true, None) =>
        new AllSetsOaiRelation(oaiConfig, oaiMethods)(sqlContext)
      case _ =>
        throw OaiHarvesterException(
          "Unable to determine harvest type from parameters."
        )
    }

  def convertToOutputRow(oaiRecordEither: Either[OaiError, OaiRecord]): Row =
    oaiRecordEither match {
      case Right(oaiRecord: OaiRecord) =>
        Row(None, oaiRecord, None)
      case Left(oaiError: OaiError) =>
        Row(None, None, oaiError)
    }

}
