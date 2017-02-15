package la.dp.ingestion3

import java.io.File
import java.util.concurrent.TimeUnit

import com.databricks.spark.avro.SchemaConverters
import la.dp.ingestion3.utils.OaiRdd
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.log4j.LogManager
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._

/**
  * Entry point for running an OAI harvest
  *
  * args Output directory: String
  *             OAI URL: String
  *             Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
  *             OAI Verb: String ListRecords, ListSets etc.
  */
object OaiHarvesterMain extends App {

  def getSchema(): StructType = {
    val fields = StructType(
      StructField("id", StringType, true) ::
      StructField("document", StringType, true) ::
      StructField("mimetype", StringType, true) :: Nil
    )

    val s = StructType(
      StructField("namespace", StringType, true) ::
      StructField("type", StringType, true) ::
      StructField("name", StringType, true) ::
      StructField("doc", StringType, true) ::
      StructField("fields",fields, true) :: Nil
    )

    s
  }

  val schemaStr =
    """
      |{
      |  "namespace": "la.dp.avro.MAP_3.1",
      |  "type": "record",
      |  "name": "OriginalRecord",
      |  "doc": "",
      |  "fields": [
      |    {"name": "document", "type": "string", "default": ""},
      |    {"name": "id", "type": "string", "default": ""},
      |    {"name": "mimetype", "type": "string"}
      |  ]
      |}
    """.stripMargin

  val logger = LogManager.getLogger("OaiResponseProcessor")

  // Complains about not being typesafe...
  if(args.length != 4 ) {
    logger.error("Bad number of args: <OUTPUT FILE>, <OAI URL>, <METADATA PREFIX>, <OAI VERB>")
    sys.exit(-1)
  }

  val urlBuilder = new OaiQueryUrlBuilder
  val outputFile = args(0)
  val oaiParams = Map[String,String](
    "endpoint" -> args(1),
    "metadataPrefix" -> args(2),
    "verb" -> args(3))



  val sparkConf = new SparkConf()
    .setAppName("Oai Harvest")
    .setMaster("local")

  val sc = new SparkContext(sparkConf)


  val spark = SparkSession.builder().master("local").getOrCreate()


  val oaiRdd: OaiRdd = new OaiRdd(sc, oaiParams, urlBuilder)

  val scheme = new Schema.Parser().parse(schemaStr)
  val structSchema = SchemaConverters.toSqlType(scheme)
  
  oaiRdd.saveAsTextFile(outputFile, classOf[org.apache.hadoop.io.compress.GzipCodec])

  sc.stop()

  getAvroCount(new File(outputFile + "/part-00000.deflate"))

  /**
    * Print the results of a harvest
    *
    * Example:
    *   Harvest count: 242924 records harvested
    *   Runtime: 4 minutes 24 seconds
    *   Throughput: 920 records/second
    *
    * @param runtime Runtime in milliseconds
    * @param recordsHarvestedCount Number of records in the output directory
    */

  def printResults(runtime: Long, recordsHarvestedCount: Long): Unit = {
    // Make things pretty
    val formatter = java.text.NumberFormat.getIntegerInstance
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(runtime)
    val seconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))
    val runtimeInSeconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) + 1
    // add 1 to avoid divide by zero error
    val recordsPerSecond: Long = recordsHarvestedCount/runtimeInSeconds

    println(s"File count: ${formatter.format(recordsHarvestedCount)}")
    println(s"Runtime: $minutes:$seconds")
    println(s"Throughput: ${formatter.format(recordsPerSecond)} records/second")
  }

  /**
    * Counts the number of items in a Avro file
    *
    * @param path File
    *             Path to the Avro file
    * @return Integer
    *         The count of items
    */
  def getAvroCount(path: File): Integer = {
    -1
  }
}
