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
  *
  * For argument options, @see OaiHarvesterConf.
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

    readerOptions("verb") match {
      // Write records to avro.
      // This task may require a large amount of driver memory.
      case "ListRecords" => {
        println(schema)

        dataframe.write
          .format("com.databricks.spark.avro")
          .option("avroSchema", schema)
          .avro(oaiConf.outputDir())
      }
      // Write sets to csv.
      case "ListSets" => {
        println(schema)

        dataframe.coalesce(1).write
          .format("com.databricks.spark.csv")
          .option("header", true)
          .csv(oaiConf.outputDir())
      }
      case _ => throw new IllegalArgumentException("Verb not recognized.")
    }

    // Stop spark session.
    sc.stop()

    val end = System.currentTimeMillis()

    Utils.printResults((end-start),recordsHarvestedCount)
  }
}
