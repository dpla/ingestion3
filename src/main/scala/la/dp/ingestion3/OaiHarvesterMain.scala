package la.dp.ingestion3

import java.io.File
import java.util.concurrent.TimeUnit

import la.dp.ingestion3.utils.OaiRdd
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Entry point for running an OAI harvest
  *
  * args Output directory: String
  *             OAI URL: String
  *             Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
  *             OAI Verb: String ListRecords, ListSets etc.
  */
object OaiHarvesterMain extends App {

  val logger = LogManager.getLogger("OaiResponseProcessor")

  // Complains about not being typesafe...
  if(args.length != 4 ) {
    logger.error("Bad number of args: <OUTPUT FILE>, <OAI URL>, <METADATA PREFIX>, <OAI VERB>")
    sys.exit(-1)
  }
  // Schema taken from http://bit.ly/2k8II6Q
  val schemaStr =
    """
      |{
      |  "namespace": "la.dp.avro.MAP_3.1",
      |  "type": "record",
      |  "name": "OriginalRecord",
      |  "doc": "",
      |  "fields": [
      |    {"name": "or_document", "type": "string", "default": ""},
      |    {"name": "or_mimetype", "type": "string", "default": ""},
      |    {"name": "id",          "type": "string"},
      |    {"name": "rdf_document","type": "string", "default": ""}
      |  ]
      |}
    """.stripMargin

  // val outputFile = new File(args(0))
  // val schema     = new Schema.Parser().parse(schemaStr)
  // val fileIO     = new AvroFileIO(schema, outputFile)
  val urlBuilder = new OaiQueryUrlBuilder
  val params     = Map[String,String](
    "endpoint" -> args(1),
    "metadataPrefix" -> args(2),
    "verb" -> args(3))





  /*
  try {
    for (record <- new OaiFeedTraversable(params, urlBuilder))
      record.map { case(id, data) => fileIO.writeFile(id, data) }
  } finally {
    fileIO.close
  }

  print(getAvroCount(outputFile, schema))
  */

  // delete the file on run
  new File(args(0)).delete()

  val sparkConf = new SparkConf()
    .setAppName("Harvester")
    .setMaster("local")

  val sc = new SparkContext(sparkConf)
  val oaiRdd: OaiRdd = new OaiRdd(sc, params, urlBuilder)
  oaiRdd.saveAsTextFile(args(0), classOf[org.apache.hadoop.io.compress.DefaultCodec])
  sc.stop()

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
    * @param schema String
    *               Avro file schema
    * @return Integer
    *         The count of items
    */
  def getAvroCount(path: File, schema: Schema): Integer = {
    var cnt = 0
    val reader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](path, reader)

    while(dataFileReader.hasNext) {
      dataFileReader.next()
      cnt = cnt+1
    }
    cnt
  }
}
