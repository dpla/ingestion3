package la.dp.ingestion3

import java.io.File
import java.util.concurrent.TimeUnit

import la.dp.ingestion3.harvesters.{OAIFeedTraversable, OAIResponseTraversable}
import la.dp.ingestion3.utils.AvroFileIO
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.log4j.LogManager

/**
  * Entry point for running an OAI harvest
  *
  * args Output directory: String
  *             OAI URL: String
  *             Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
  *             OAI Verb: String ListRecords, ListSets etc.
  */
object OaiHarvesterMain extends App {

  val logger = LogManager.getLogger("OAIResponseTraversable")

  // Complains about not being typesafe...
  if(args.length != 4 ) {
    logger.error("Bad number of args: <OUTPUT FILE>, <OAI URL>, <METADATA PREFIX>, <OAI VERB>")
    sys.exit(-1)
  }

  val outputFile: File = new File(args(0))
  val queryUrlBuilder = new OaiQueryUrlBuilder
  val fileIO = new AvroFileIO(avrscheme, outputFile)
  val oaiRunner = new OAIFeedTraversable()

  val params = Map[String,String](
    "endpoint" -> args(1),
    "metadataPrefix" -> args(2),
    "verb" -> args(3))

  val avrscheme =
    """
      | {
      |   "namespace": "dpla",
      |   "type": "record",
      |   "name": "OriginalRecord",
      |   "fields": [
      |     {"name": "body", "type": "string"}
      |   ]
    |   }
    """.stripMargin

  for (response <- oaiRunner.go(params, queryUrlBuilder)) {
    for (record <- new OAIResponseTraversable(response)) {
      fileIO.writeFile(record)
    }
  }

  print(getAvroCount(outputFile, avrscheme))

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
  def getAvroCount(path: File, schema: String): Integer = {
    var cnt = 0
    val schema_obj =  new Schema.Parser
    val schema2 = schema_obj.parse(schema)
    val READER2 = new GenericDatumReader[GenericRecord](schema2)

    val dataFileReader = new DataFileReader[GenericRecord](path, READER2)

    while(dataFileReader.hasNext) {
      cnt = cnt+1
      dataFileReader.next()
    }
    cnt
  }
}
