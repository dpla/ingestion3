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
  * Entry point for running an OAI harvest of records
  *
  * args Output directory: String
  *             OAI URL: String
  *             Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
  *             Provider: Provider name (we need to standardize this).
  *             Sets (Optional): Comma-separated String of sets to harvest from.
  */
object OaiRecordHarvesterMain {

  val schemaStr =
    """{
        "namespace": "la.dp.avro",
        "type": "record",
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
    """//.stripMargin // TODO we need to template the document field so we can record info there

  val logger = LogManager.getLogger(OaiRecordHarvesterMain.getClass)

  def main(args: Array[String]): Unit = {
    val oaiConf = new OaiHarvesterConf(args.toSeq)
    val verb = "ListRecords"

    Utils.deleteRecursively(new File(oaiConf.outputDir()))

    // Initiate spark session.
    val sparkConf = new SparkConf().setAppName("Oai Record Harvest")
    // sparkMaster has a default value of local[*] if not provided.
    // TODO: will this default value work with EMR?
    sparkConf.setMaster(oaiConf.sparkMaster())

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val start = System.currentTimeMillis()

    val baseOptions = Map("metadataPrefix" -> oaiConf.prefix(), "verb" -> verb)
    val readerOptions = getReaderOptions(baseOptions, oaiConf.sets.toOption, oaiConf.blacklist.toOption)

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

  /**
    *
    * @param baseOptions Existing Map of OAI harvester options
    * @param sets Comma separated list of sets
    * @param blacklist Comma separated list of sets to not harvest
    * @return Updated Map of OAI harvester options
    */
  def getReaderOptions(baseOptions: Map[String, String],
                       sets: Option[String],
                       blacklist: Option[String]): Map[String, String] = {

    (sets, blacklist) match {
      // No blacklist, harvest all sets
      case (Some(sets), None) => baseOptions + ("sets" -> sets)
      // No set list or blacklist, harvest all records
      case (None, None) => baseOptions
      // Remove blacklisted sets from set list
      case (Some(sets), Some(blacklist)) => {
        baseOptions + ("sets" -> (sets.split(",") filter (!blacklist.split(",").contains(_))).mkString(","))
      }
      // TODO: Call ListSets to generate a set list and returns a second call to getReadOptions() with the set list
      case (None, Some(blacklist)) => {
        baseOptions
      }
    }
  }
}
