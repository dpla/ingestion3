package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.{FlatFileIO, Utils}
import com.databricks.spark.avro._
import dpla.ingestion3.confs.OaiHarvesterConf
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


/**
  * Entry point for running an OAI harvest.
  */
object OaiHarvesterMain {
  
  val logger = LogManager.getLogger(OaiHarvesterMain.getClass)

  def main(args: Array[String]): Unit = {
    val oaiConf = new OaiHarvesterConf(args.toSeq)

    Utils.deleteRecursively(new File(oaiConf.outputDir()))

    // Initiate spark session.
    val sparkConf = new SparkConf().setAppName("Oai Harvest")
    // sparkMaster has a default value of local[*] if not provided.
    // TODO: will this default value work with EMR?
    sparkConf.setMaster(oaiConf.sparkMaster())

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val schema = new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc")

    val start = System.currentTimeMillis()

    val readerOptions: Map[String, String] = Map(
      "verb" -> oaiConf.verb.toOption,
      "metadataPrefix" -> oaiConf.prefix.toOption,
      "harvestAllSets" -> oaiConf.harvestAllSets.toOption,
      "setlist" -> oaiConf.setlist.toOption,
      "blacklist" -> oaiConf.blacklist.toOption
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    val results = spark.read
      .format("dpla.ingestion3.harvesters.oai")
      .options(readerOptions)
      .load(oaiConf.endpoint())

    results.persist(StorageLevel.DISK_ONLY)

    val dataframe = results.withColumn("provider", lit(oaiConf.provider()))
      .withColumn("mimetype", lit("application_xml"))

    val recordsHarvestedCount = dataframe.count()

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

    val end = System.currentTimeMillis()

    Utils.printResults((end-start),recordsHarvestedCount)
  }
}