package dpla.ingestion3.harvesters

import java.io.File

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

abstract class Harvester(spark: SparkSession,
                         shortName: String,
                         conf: i3Conf,
                         outStr: String,
                         logger: Logger) {
  def harvest: Try[Long] = {
    val start = System.currentTimeMillis()

    // Call local implementation of runHarvest()
    Try {
      // Calls the local implementation
      val harvestData: DataFrame = localHarvest()

      // Write harvested data to output file.
      harvestData
        .write
        .format("com.databricks.spark.avro")
        .option("avroSchema", harvestData.schema.toString)
        .avro(outStr)

      logger.info(s"Saving to $outStr")
      cleanUp()

      // Reads the saved avro file back
      spark.read.avro(outStr)
    } match {
      case Success(df) =>
        Harvester.validateSchema(df)
        val recordCount = df.count()
        logger.info(Utils.harvestSummary(System.currentTimeMillis() - start, recordCount))
        Success(recordCount)
      case Failure(f) => Failure(f)
    }
  }

  def mimeType: String

  def localHarvest(): DataFrame

  def cleanUp(): Unit = Unit

}
/**
  * Abstract class for local harvesters.
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
abstract class LocalHarvester(
                               spark: SparkSession,
                               shortName: String,
                               conf: i3Conf,
                               outStr: String,
                               logger: Logger)
  extends Harvester(spark, shortName, conf, outStr, logger) {

  // Temporary output path.
  // Harvests that use AvroWriter cannot be written directly to S3.
  // Instead, they are written to this temp path,
  //   then loaded into a spark DataFrame,
  //   then written to their final destination.
  val tmpOutStr = s"/tmp/$shortName"

  // Delete temporary output directory and files if they already exist.
  Utils.deleteRecursively(new File(tmpOutStr))

  private val avroWriter: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, tmpOutStr, Harvester.schema)

  def getAvroWriter: DataFileWriter[GenericRecord] = avroWriter

  override def cleanUp(): Unit = {
    avroWriter.close()
    // Delete temporary output directory and files.
    Utils.deleteRecursively(new File(tmpOutStr))
  }
}

object Harvester {

  // Schema for harvested records.
  val schema: Schema =
    new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc"))

  def validateSchema(df: DataFrame): Unit = {
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
      Logger.getLogger(Harvester.getClass).warn(msg)
    }
  }
}

