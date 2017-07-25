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
  *
  * For argument options, @see OaiHarvesterConf.
  */
object OaiHarvesterMain {

  val recordSchemaStr =
    """{
        "namespace": "dpla.avro",
        "type": "record",
        "name": "OriginalRecord.v1",
        "doc": "",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "document", "type": "string"},
          {"name": "set_id", "type": "string"},
          ("name": "set_document", "type": "string"},
          {"name": "provider", "type": "string"},
          {"name": "mimetype", "type": { "name": "MimeType",
           "type": "enum", "symbols": ["application_json", "application_xml", "text_turtle"]}
           }
        ]
      }
    """//.stripMargin // TODO we need to template the document field so we can record info there

  // This schema String is printed to help with debugging.
  // It is NOT implemented during the write operation b/c sets are written to CSV.
  val setSchemaStr =
    """{
        "namespace": "dpla.avro",
        "type": "set",
        "name": "OriginalRecord.v1",
        "doc": "",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "document", "type": "string"},
          {"name": "provider", "type": "string"},
          {"name": "mimetype", "type": { "name": "MimeType",
           "type": "enum", "symbols": ["application_json", "application_xml", "text_turtle"]}
           }
        ]
      }
    """

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

    val readerOptions: Map[String, String] = Map(
      "verb" -> oaiConf.verb.toOption,
      "metadataPrefix" -> oaiConf.prefix.toOption,
      "harvestAllSets" -> oaiConf.harvestAllSets.toOption,
      "setlist" -> oaiConf.setlist.toOption,
      "blacklist" -> oaiConf.blacklist.toOption,
      "endpoint" -> oaiConf.endpoint.toOption
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    def runHarvest(): Try[DataFrame] = {
      Try(spark.read
        .format("dpla.ingestion3.harvesters.oai")
        .options(readerOptions)
        .load())
    }

    runHarvest() match {
      case Success(results) => {
        results.persist(StorageLevel.DISK_ONLY)

        val dataframe = results.withColumn("provider", lit(oaiConf.provider()))
          .withColumn("mimetype", lit("application_xml"))

        // Log the results of the harvest
        logger.info(s"Harvested ${dataframe.count()} records")

        readerOptions("verb") match {
          // Write records to avro.
          // This task may require a large amount of driver memory.
          case "ListRecords" => {
            println(recordSchemaStr)

            dataframe.write
              .format("com.databricks.spark.avro")
              .option("avroSchema", recordSchemaStr)
              .avro(oaiConf.outputDir())
          }
          // Write sets to csv.
          case "ListSets" => {
            println(setSchemaStr)

            dataframe.coalesce(1).write
              .format("com.databricks.spark.csv")
              .option("header", true)
              .csv(oaiConf.outputDir())
          }
        }
      }
      case Failure(f) => logger.fatal(s"Unable to harvest records. ${f.getMessage}")
    }
    // Stop spark session.
    sc.stop()
  }
}
