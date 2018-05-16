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
  * @param conf [i3Conf] contains configs for the harvester.
  * @param outStr [String] outputPathStr for the harvested data.
  * @param logger [Logger] for the harvester.
  */
abstract class Harvester(shortName: String,
                         conf: i3Conf,
                         outStr: String,
                         logger: Logger)
  extends AwsUtils {

  private val avroWriter: DataFileWriter[GenericRecord]  = {
    val filename = s"${shortName}_${System.currentTimeMillis()}.avro"
    val path = if (outStr.endsWith("/")) outStr else outStr + "/"
    val outputDir = new File(path)
    outputDir.mkdirs()
    if (!outputDir.exists) throw new RuntimeException(s"Output directory ${path} does not exist")
    logger.info(s"Writing output to ${path}")

    val avroWriter = AvroUtils.getAvroWriter(new File(path + filename), schema)
    avroWriter.setFlushOnEveryBlock(true)
    avroWriter
  }

  protected def getAvroWriter(): DataFileWriter[GenericRecord] = avroWriter

  protected lazy val fileIo = new FlatFileIO()


  /**
    * Abstract method mimeType should store the mimeType of the harvested data.
    */
  protected val mimeType: String

  protected lazy val outputPath = new File(outStr)

  protected lazy val schemaStr: String = fileIo.readFileAsString("/avro/OriginalRecord.avsc")
  protected lazy val schema: Schema = new Schema.Parser().parse(schemaStr)

  /**
    * Initiate a spark session using the configs specified in the i3Conf.
    *
    * Lazy evaluation b/c some harvesters do not need a spark context until the
    * very end of the harvest.
    *
    * @return SparkSession
    */
  protected lazy val sc: SparkContext = spark.sparkContext

  protected lazy val spark: SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(s"Harvest: $shortName")

    val sparkMaster = conf.spark.sparkMaster.getOrElse("local[1]")
    sparkConf.setMaster(sparkMaster)

    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  // Only set AWS key properties in SparkContext if output destination is s3
  if (outStr.startsWith("s3")) {
    sc.hadoopConfiguration.set("fs.s3a.access.key", s3AccessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", s3SecretKey)
  }

  /**
    * Entry point for performing harvest. Deletes existing data on path,
    * executes harvest, validates result against schema and returns the
    * count of successfully harvested records or failures.
    *
    * @return Try[Long] Number of harvested records or Failure
    */
  def harvest: Try[Long] = {
    val start = System.currentTimeMillis()

    // Call local implementation of runHarvest()
    val harvestResult = runHarvest match {
      case Success(df) =>
        validateSchema(df)
        val recordCount = df.count()
        logger.info(Utils.harvestSummary(System.currentTimeMillis()-start, recordCount))
        Success(recordCount)
      case Failure(f) => Failure(f)
    }
    // Shut down spark session.
    sc.stop()
    // Count of record or Failure
    harvestResult
  }

  /**
    * Performs specific harvest (OAI, p2p, Cdl, PSS)
    *
    * To be defined in implementing class
    */
  protected def localHarvest(): Unit

  /**
    * Generalized driver for FileHarvesters invokes localApiHarvest() method and reports
    * summary information.
    */
  protected def runHarvest: Try[DataFrame] = Try {
    // Calls the local implementation
    localHarvest()
    avroWriter.close()
    logger.info(s"Saving to $outStr")
    // Reads the saved avro file back
    spark.read.avro(outStr)
  }

  /**
    * Check that harvested DataFrame meets the expected schema.
    * If not, log a warning.
    * This is for debugging - it will not stop a harvest from completing.
    *
    * @param df [DataFrame] The final DataFrame from the harvest.
    */
  protected def validateSchema(df: DataFrame): Unit = {
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
      fields.map{ s => s.name -> s.dataType }

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

/**
  * Generic URL builder. Implemented in harvesters that need some help when building
  * HTTP requests.
  */
trait UrlBuilder {
  protected def buildUrl(params: Map[String, String]): URL
}
