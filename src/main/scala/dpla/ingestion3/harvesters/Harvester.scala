package dpla.ingestion3.harvesters

import java.io.File

import com.amazonaws.auth.{AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.Utils
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
  * @param conf [i3Conf] contains configs for the harvester.
  * @param outputDir [String] outputDir for the harvested data.
  * @param harvestLogger [Logger] for the harvester.
  */
abstract class Harvester(shortName: String,
                         conf: i3Conf,
                         outputDir: String,
                         harvestLogger: Logger) {
  /**
    * Abstract method doHarvest should:
    *   1. Run a harvest.
    *   2. Save harvested data.
    *   3. Return a Try[DataFrame] of the harvested data.
    *
    * @return Try[DataFrame]
    */
  protected def runHarvest: Try[DataFrame]

  /**
    * Abstract method mimeType should store the mimeType of the harvested data.
    */
  protected val mimeType: String

  /**
    * Entry point for running a harvest.
    */
  def harvest = {

    // If the output directory already exists it is a local path
    // then delete it and its contents.
    // TODO Move this into a shell script
    outputFile.getParentFile.mkdirs()
    if (outputFile.exists & !outputDir.startsWith("s3"))
      harvestLogger.info(s"Output directory already exists. Deleting ${outputDir}...")
      Utils.deleteRecursively(outputFile)

    val startTime = System.currentTimeMillis()

    // Call local implementation.
    runHarvest match {

      case Success(df) =>
        // Log details about the successful harvest.
        val endTime = System.currentTimeMillis()
        harvestLogger.info(Utils.logResults(endTime-startTime, df.count()))
        validateSchema(df)

      case Failure(f) =>
        // Log the failure.
        harvestLogger.fatal(s"Unable to harvest records. ${f.getMessage}")
    }

    // Shut down spark session.
    sc.stop()
  }

  protected val outputFile = new File(outputDir)
  protected lazy val s3AccessKey: String = awsCredentials.getCredentials.getAWSAccessKeyId
  protected lazy val s3SecretKey: String = awsCredentials.getCredentials.getAWSSecretKey

  /**
    * DefaultAWSCredentialsProviderChain looks for AWS keys in the following order:
    *   1. Environment Variables
    *   2. Java System Properties
    *   3. Credential profiles file at the default location (~/.aws/credentials)
    *   4. Instance profile credentials delivered through the Amazon EC2 metadata service
    *
    * @return
    */
  def awsCredentials = new AWSCredentialsProviderChain(new DefaultAWSCredentialsProviderChain)

  /**
    * Initiate a spark session using the configs specified in the i3Conf.
    *
    * Lazy evaluation b/c some harvesters do not need a spark context until the
    * very end of the harvest.
    *
    * @return SparkSession
    */
  protected lazy val spark: SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(s"Harvest: ${shortName}")

    val sparkMaster = conf.spark.sparkMaster.getOrElse("local[1]")
    sparkConf.setMaster(sparkMaster)

    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  protected lazy val sc: SparkContext = spark.sparkContext

  // Only set AWS keys if output destination is s3
  if (outputDir.startsWith("s3")) {
    sc.hadoopConfiguration.set("fs.s3a.access.key", s3AccessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", s3SecretKey)
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
      harvestLogger.warn(msg)
    }
  }
}


object HarvesterExceptions {

  def throwMissingArgException(arg: String) = {
    val msg = s"Missing argument: $arg"
    throw new IllegalArgumentException(msg)
  }

  def throwUnrecognizedArgException(arg: String) = {
    val msg = s"Unrecognized argument: $arg"
    throw new IllegalArgumentException(msg)
  }

  def throwValidationException(arg: String) = {
    val msg = s"Validation error: ${arg}"
    throw new IllegalArgumentException(msg)
  }
}