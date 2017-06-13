package dpla.ingestion3

import java.io.File

import dpla.ingestion3.utils.Utils
import com.databricks.spark.avro._
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Entry point for running a Primary Source Set (PSS) harvest
  *
  * args Output directory: String
  *             PSS URL: String
  */
object PssHarvesterMain {

  val schemaStr =
    """{
        "namespace": "la.dp.avro",
        "type": "primary source set",
        "doc": "",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "document", "type": "string"},
          {"name": "mimetype", "type": { "name": "MimeType",
           "type": "enum", "symbols": ["application_json", "application_xml", "text_turtle"]}
           }
        ]
      }
    """//.stripMargin

  val logger = LogManager.getLogger(PssHarvesterMain.getClass)

  def main(args: Array[String]): Unit = {

    validateArgs(args)
    println(schemaStr)

    val outputFile = args(0)
    val endpoint = args(1)

    Utils.deleteRecursively(new File(outputFile))

    val sparkConf = new SparkConf().setAppName("PSS Harvest")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val start = System.currentTimeMillis()

    val results = spark.read
      .format("dpla.ingestion3.harvesters.pss")
      .load(endpoint)

    val dataframe = results.withColumn("mimetype", lit("application_json"))

    val recordsHarvestedCount = dataframe.count()

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
    if(args.length < 2) {
      logger.error("Bad number of args: <OUTPUT FILE>, <PSS URL>")
      sys.exit(-1)
    }
  }
}
