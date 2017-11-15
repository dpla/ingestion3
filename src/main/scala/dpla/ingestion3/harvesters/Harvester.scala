package dpla.ingestion3.harvesters

import java.io.File
import java.net.URL

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.api.{ApiError, ApiRecord, ApiResponse}
import dpla.ingestion3.utils.{AvroUtils, AwsUtils, FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Abstract class for all harvesters.
  *
  * The harvester abstract class has methods to manage aspects of a harvest
  * that are common among all providers, including:
  *   1. Provide an entry point to run a harvest.
  *   2. If the output directory already exists, delete its contents.
  *   3. Log and timestamp the beginning and end of a harvest.
  *   4. Log information about the completed harvest.
  *   5. Validate the schema final DataFrame (only for logging purposes).
  *   6. Manage the spark session.
  *
  * @param shortName [String] Provider short name
  * @param conf      [i3Conf] contains configs for the harvester.
  * @param outStr    [String] outputPathStr for the harvested data.
  * @param logger    [Logger] for the harvester.
  */
abstract class Harvester(spark: SparkSession,
                         shortName: String,
                         conf: i3Conf,
                         outStr: String,
                         logger: Logger) {



  private val avroWriter: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, outStr, Harvester.schema)

  def getAvroWriter: DataFileWriter[GenericRecord] = avroWriter

  /**
    * Abstract method mimeType should store the mimeType of the harvested data.
    */
  protected val mimeType: String

  protected lazy val outputPath = new File(outStr)

  /**
    * Entry point for performing harvest. Executes harvest, validates result
    * against schema and returns the count of successfully harvested records
    * or failures. Defers to localHarvest implementation to gather actual
    * records.
    *
    * @return Try[Long] Number of harvested records or Failure
    */
  def harvest: Try[Long] = {
    val start = System.currentTimeMillis()

    // Call local implementation of runHarvest()
    Try {
      // Calls the local implementation
      localHarvest()
      logger.info(s"Saving to $outStr")
      // Reads the saved avro file back
      avroWriter.close()
      spark.read.avro(outStr)
    } match {
      case Success(df) =>
        validateSchema(df)
        val recordCount = df.count()
        logger.info(Utils.harvestSummary(System.currentTimeMillis() - start, recordCount))
        Success(recordCount)
      case Failure(f) => Failure(f)
    }
  }

  /**
    * Template method for implementations to override. Expectation is that
    * at the end of the execution of this method, avro files in the correct
    * schema will be available in the folder at path outStr.
    *
    * To be defined in implementing class
    */
  protected def localHarvest(): Unit

  /**
    * Check that harvested DataFrame meets the expected schema.
    * If not, log a warning.
    * This is for debugging - it will not stop a harvest from completing.
    *
    * @param df [DataFrame] The final DataFrame from the harvest.
    */
  private def validateSchema(df: DataFrame): Unit = {
    val idSt = StructField("id", StringType, true)
    val docSt = StructField("document", StringType, true)
    val dateSt = StructField("ingestDate", LongType, false)
    val provSt = StructField("provider", StringType, false)
    val mimeSt = StructField("mimetype", StringType, false)

    // Match the fields within the schema, rather than the schema itself.
    // This allows DataFrames where the fields are in different orders to pass
    // the logical test.
    val expectedStructs = Array(idSt, docSt, dateSt, provSt, mimeSt)
    val actualStructs = df.schema.fields

    // Match only the names and data types of the fields.
    // Whether or not a field is nullable does not matter for our purposes.
    def mapFields(fields: Array[StructField]): Array[(String, DataType)] =
      fields.map { s => s.name -> s.dataType }

    val expectedFields = mapFields(expectedStructs)
    val actualFields = mapFields(actualStructs)

    if (actualFields.diff(expectedFields).size > 0) {
      val msg =
        s"""Harvested DataFrame did not match expected schema.\n
        Actual fields: ${actualFields.mkString(", ")}\n
        Expected fields: ${expectedFields.mkString(", ")}"""
      logger.warn(msg)
    }
  }
}

object Harvester {

  // Schema for harvested records.
  val schema: Schema =
    new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc"))
}

