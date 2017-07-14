package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.Utils
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

    val start = System.currentTimeMillis()

    val readerOptions: Map[String, String] = Map(
      "verb" -> oaiConf.verb.toOption,
      "metadataPrefix" -> oaiConf.prefix.toOption,
      "harvestAllSets" -> oaiConf.harvestAllSets.toOption,
      "setlist" -> oaiConf.setlist.toOption,
      "blacklist" -> oaiConf.blacklist.toOption
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    val schemaStr = readerOptions("verb") match {
      case "ListRecords" => recordSchemaStr
      case "ListSets" => setSchemaStr
      case _ => throw new IllegalArgumentException("Verb not recognized.")
    }

    val results = spark.read
      .format("dpla.ingestion3.harvesters.oai")
      .options(readerOptions)
      .load(oaiConf.endpoint())

    results.persist(StorageLevel.DISK_ONLY)

    val dataframe = results.withColumn("provider", lit(oaiConf.provider()))
      .withColumn("mimetype", lit("application_xml"))

    val recordsHarvestedCount = dataframe.count()

    // This task may require a large amount of driver memory.
    dataframe.write
      .format("com.databricks.spark.avro")
      .option("avroSchema", schemaStr)
      .avro(oaiConf.outputDir())

    // Stop spark session.
    sc.stop()

    val end = System.currentTimeMillis()

    Utils.printResults((end-start),recordsHarvestedCount)
  }
}
