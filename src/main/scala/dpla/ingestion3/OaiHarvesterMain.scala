package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.Utils
import com.databricks.spark.avro._
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


/**
  * Entry point for running an OAI harvest
  *
  * args Output directory: String
  *             OAI URL: String
  *             Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
  *             OAI Verb: String ListRecords, ListSets etc.
  *             Provider: Provider name (we need to standardize this)
  */
object OaiHarvesterMain {

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

  val logger = LogManager.getLogger(OaiHarvesterMain.getClass)

  def main(args: Array[String]): Unit = {

    validateArgs(args)
    println(schemaStr)

    val outputFile = args(0)
    val endpoint = args(1)
    val metadataPrefix = args(2)
    val verb = args(3)
    val provider = args(4)
    val sets: Option[String] = if (args.isDefinedAt(5)) Some(args(5)) else None

    Utils.deleteRecursively(new File(outputFile))

    val sparkConf = new SparkConf().setAppName("Oai Harvest")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val start = System.currentTimeMillis()

    val baseOptions = Map( "metadataPrefix" -> metadataPrefix, "verb" -> verb)
    val readerOptions = getReaderOptions(baseOptions, sets)

    val results = spark.read
      .format("dpla.ingestion3.harvesters.oai")
      .options(readerOptions)
      .load(endpoint)

    results.persist(StorageLevel.DISK_ONLY)

    val dataframe = results.withColumn("provider", lit(provider))
      .withColumn("mimetype", lit("application_xml"))

    val recordsHarvestedCount = dataframe.count()

    // This task may require a large amount of driver memory.
    dataframe.write
      .format("com.databricks.spark.avro")
      .option("avroSchema", schemaStr)
      .avro(outputFile)

    sc.stop()

    val end = System.currentTimeMillis()

    Utils.printResults((end-start),recordsHarvestedCount)
  }

  def validateArgs(args: Array[String]) = {
    // Complains about not being typesafe...
    if(args.length < 5) {
      logger.error("Bad number of args: <OUTPUT FILE>, <OAI URL>, " +
        "<METADATA PREFIX>, <OAI VERB>, <PROVIDER>, " +
        "<SETS> (optional)")
      sys.exit(-1)
    }
  }

  def getReaderOptions(baseOptions: Map[String, String],
                       sets: Option[String]): Map[String, String] = {
    sets match {
      case Some(sets) => baseOptions + ("sets" -> sets)
      case None => baseOptions
    }
  }
}
