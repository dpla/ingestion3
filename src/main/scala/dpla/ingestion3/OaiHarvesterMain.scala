package dpla.ingestion3

import java.io.File
import dpla.ingestion3.utils.Utils
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

  Utils.deleteRecursively(new File(outputFile))

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

  // This task may require a large amount of driver memory.
  dataframe.write
           .format("com.databricks.spark.avro")
           .option("avroSchema", schemaStr)
           .avro(outputFile)

  sc.stop()

  val end = System.currentTimeMillis()

  Utils.printResults((end-start),recordsHarvestedCount)
}
