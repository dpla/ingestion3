package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.Utils
import com.databricks.spark.avro._
import dpla.ingestion3.confs.OaiHarvesterConf
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
    val oaiConf = new OaiHarvesterConf(args.toSeq)
    val oaiParams = oaiConf.load()

    // TODO print something pleasant.
    Utils.deleteRecursively(new File(oaiParams.outputDir.get))

    // Initiate spark session.
    val sparkConf = new SparkConf().setAppName("Oai Harvest")
    // sparkMaster has a default value of local[*] if not provided.
    // TODO: will this default value work with EMR?
    sparkConf.setMaster(oaiParams.sparkMaster.get)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    // Set options
    val readerOptions: Map[String, String] = Map(
      "verb" -> oaiParams.verb,
      "metadataPrefix" -> oaiParams.metadataPrefix,
      "harvestAllSets" -> oaiParams.harvestAllSets,
      "setlist" -> oaiParams.setList,
      "blacklist" -> oaiParams.blacklist,
      "endpoint" -> oaiParams.endpoint
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    // These were already validated in OaiHarvesterConf so this is redundant but did not want
    // to call .get() on an option to access the properties when saving the avro
    val outputDir = oaiParams.outputDir match {
      case Some(d) => d
      case _ => throw new IllegalArgumentException("Output directory is not specified. Terminating run.")
    }

    val provider = oaiParams.provider match {
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