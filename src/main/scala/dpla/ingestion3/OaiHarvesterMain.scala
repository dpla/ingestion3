package dpla.ingestion3

import java.io.File
import java.util.concurrent.TimeUnit

import com.databricks.spark.avro._
import org.apache.avro.Schema
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
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
object OaiHarvesterMain extends App {
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

  // Complains about not being typesafe...
  if(args.length != 5 ) {
    logger.error("Bad number of args: <OUTPUT FILE>, <OAI URL>, <METADATA PREFIX>, <OAI VERB>, <PROVIDER>")
    sys.exit(-1)
  }

  println(schemaStr)

  val outputFile = args(0)
  val endpoint = args(1)
  val metadataPrefix = args(2)
  val verb = args(3)
  val provider = args(4)

  deleteRecursively(new File(outputFile))

  val sparkConf = new SparkConf()
    .setAppName("Oai Harvest")
    .setMaster("local") //todo parameterize

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext

  val start = System.currentTimeMillis()

  val avroSchema = new Schema.Parser().parse(schemaStr)
  val schemaType = SchemaConverters.toSqlType(avroSchema)
  val structSchema = schemaType.dataType.asInstanceOf[StructType]

  val oaiResults = spark.read
                        .format("dpla.ingestion3.harvesters.oai")
                        .option("metadataPrefix", metadataPrefix)
                        .option("verb", verb)
                        .load(endpoint)

  oaiResults.persist(StorageLevel.DISK_ONLY)

  val dataframe = oaiResults.withColumn("provider", lit(provider))
                            .withColumn("mimetype", lit("application_xml"))

  val recordsHarvestedCount = dataframe.count()

  dataframe.write
           .format("com.databricks.spark.avro")
           .option("avroSchema", schemaStr)
           .avro(outputFile)

  sc.stop()

  val end = System.currentTimeMillis()

  printResults((end-start),recordsHarvestedCount)

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
    * Delete a directory
    * Taken from http://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala#25999465
    * @param file
    */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}
