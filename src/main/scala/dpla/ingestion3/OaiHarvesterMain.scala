package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.Utils
import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf}

import com.databricks.spark.avro._

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

/**
  * Entry point for running an OAI harvest.
  */
object OaiHarvesterMain {

  def main(args: Array[String]): Unit = {
    // Read in command line args
    val cmdArgs = new CmdArgs(args)

    val outputDir = cmdArgs.output.getOrElse(throw new IllegalArgumentException("Missing output dir"))
    val confFile = cmdArgs.configFile.getOrElse(throw new IllegalArgumentException("Missing conf file"))
    val providerName = cmdArgs.providerName.getOrElse(throw new IllegalArgumentException("Missing provider name"))

    // Setup logger with dynamically named output file
    val harvestLogger: Logger = LogManager.getLogger("ingestion3")
    val appender = Utils.getFileAppender(providerName, "harvest")
    harvestLogger.addAppender(appender)

    // Load configuration from file
    val i3Conf = new Ingestion3Conf(confFile, Some(providerName))
    val providerConf = i3Conf.load()

    // TODO log the settings that are being used here...

    // If the output directory already exists then delete it and its contents
    if (new File(outputDir).exists())
      harvestLogger.info(s"Output directory already exists. Deleting ${outputDir}...")
      Utils.deleteRecursively(new File(outputDir))

    // Initiate spark session.
    val sparkConf = new SparkConf().setAppName("Oai Harvest")
    sparkConf.setMaster(providerConf.spark.sparkMaster.get)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    // Set options
    val readerOptions: Map[String, String] = Map(
      "verb" -> providerConf.harvest.verb,
      "metadataPrefix" -> providerConf.harvest.metadataPrefix,
      "harvestAllSets" -> providerConf.harvest.harvestAllSets,
      "setlist" -> providerConf.harvest.setlist,
      "blacklist" -> providerConf.harvest.blacklist,
      "endpoint" -> providerConf.harvest.endpoint
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    val provider = providerConf.provider match {
      case Some(p) => p
      case _ => throw new IllegalArgumentException("Provider is not specified. Terminating run.")
    }

    def runHarvest(): Try[DataFrame] = {
      Try(spark.read
        .format("dpla.ingestion3.harvesters.oai")
        .options(readerOptions)
        .load())
    }

    harvestLogger.info(s"Beginning ${provider} harvest")
    val startTime = System.currentTimeMillis()

    runHarvest() match {
      case Success(results) =>
        results.persist(StorageLevel.DISK_ONLY)

        val dataframe = results.withColumn("provider", lit(provider))
          .withColumn("mimetype", lit("application_xml"))

        // Log the results of the harvest
        val formatter = java.text.NumberFormat.getIntegerInstance
        harvestLogger.info(s"Successfully harvested ${formatter.format(dataframe.count())} records")

        val schema = dataframe.schema

        dataframe.write
          .format("com.databricks.spark.avro")
          .option("avroSchema", schema.toString)
          .avro(outputDir)

        harvestLogger.info(s"Records saved to ${outputDir}")
        val endTime = System.currentTimeMillis()
        // TODO passing *this* logger to other classes so all relevant output is in single file
        Utils.logResults(endTime-startTime, dataframe.count(), harvestLogger)

      case Failure(f) => harvestLogger.fatal(s"Unable to harvest records. ${f.getMessage}")
    }
    // Stop spark session.
    sc.stop()
  }
}