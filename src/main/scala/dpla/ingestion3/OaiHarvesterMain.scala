package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.Utils
import com.databricks.spark.avro._
import dpla.ingestion3.confs.{HarvestCmdArgs, Ingestion3Conf}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

/**
  * Entry point for running an OAI harvest.
  */
object OaiHarvesterMain {

  val logger = LogManager.getLogger(OaiHarvesterMain.getClass)

  def main(args: Array[String]): Unit = {
    val cmdArgs = new HarvestCmdArgs(args)

    val outputDir = cmdArgs.output.toOption match {
      case Some(o) => o
    }
    val confFile = cmdArgs.configFile.toOption match {
      case Some(f) => f
    }
    val providerName = cmdArgs.providerName.toOption match {
      case Some(p) => p
    }

    // Load configuration from file
    val i3Conf = new Ingestion3Conf(confFile, providerName)
    val providerConf = i3Conf.load()

    // If the output directory already exists then delete it and its contents
    if (new File(outputDir).exists())
      logger.info(s"Directory already exists. Deleting ${outputDir}...")
      Utils.deleteRecursively(new File(outputDir))

    // Initiate spark session.
    val sparkConf = new SparkConf().setAppName("Oai Harvest")
    // TODO: will this default value work with EMR?
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

    runHarvest() match {
      case Success(results) =>
        results.persist(StorageLevel.DISK_ONLY)

        val dataframe = results.withColumn("provider", lit(provider))
          .withColumn("mimetype", lit("application_xml"))

        // Log the results of the harvest
        logger.info(s"Harvested ${dataframe.count()} records")

        val schema = dataframe.schema

        dataframe.write
          .format("com.databricks.spark.avro")
          .option("avroSchema", schema.toString)
          .avro(outputDir)

      case Failure(f) => logger.fatal(s"Unable to harvest records. ${f.getMessage}")
    }
    // Stop spark session.
    sc.stop()
  }
}