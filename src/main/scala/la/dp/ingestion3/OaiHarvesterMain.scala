package la.dp.ingestion3

import java.io.File
import java.util.concurrent.TimeUnit

import la.dp.ingestion3.harvesters.OaiHarvester

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericRecordBuilder}

import scala.collection.mutable

import scala.util.control.Breaks._

/**
  * Driver program for OAI harvest
  */
object OaiHarvesterMain extends App {

  /**
    * Entry point for running an OAI harvest
    *
    * @param args Output directory: String
    *             OAI URL: String
    *             Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
    *             OAI Verb: String ListRecords, ListSets etc.
    */
  override def main(args: Array[String]): Unit = {

    val logger = org.apache.log4j.LogManager.getLogger("harvester")

    // Complains about not being typesafe...
    if(args.length != 4 ) {
      logger.error("Bad number of args: <OUT>, <OAI URL>, <METADATA PREFIX>, <OAI VERB>")
      sys.exit(-1)
    }

    val outDir: File = new File(args(0))

    val endpoint = args(1)
    val metadataPrefix = args(2)
    val verb = args(3)
    // How to serialize the output

    val queryUrlBuilder = new OaiQueryUrlBuilder
    // Create filesystem IO
    val avsc =
      """
        |{
        | "type" : "record",
        | "name" : "oaiHarvester",
        | "fields": [
        |   {
        |     "name": "data",
        |     "type" : "string"
        |   }
        | ]
        |}
      """.stripMargin

    val schema = new Schema.Parser().parse(avsc)
    val writer = new DataFileWriter[Object](new GenericDatumWriter[Object]())
    val builder = new GenericRecordBuilder(schema)
    writer.setCodec(CodecFactory.snappyCodec())
    writer.create(schema, new File("/Users/scott/test.avro"))

    logger.debug(s"Harvesting from ${endpoint}")

    // TODO I still have one mutable var here, how the F. is this supposed to work
    // TODO in terms of 'streaming' these records if the resumptionToken changes every time
    var params: mutable.Map[String, String] = mutable.Map("endpoint" -> endpoint,
                                                          "metadataPrefix" -> metadataPrefix,
                                                          "verb" -> verb)


    breakable { while(true) {
      val url = queryUrlBuilder.buildQueryUrl(params.toMap)

      logger.debug( url.toString )

      val oaiHarvester = new OaiHarvester()
      val xml = oaiHarvester.getXmlResponse(url)
      // Evaluates whether the harvest is done
      oaiHarvester.getResumptionToken(xml) match {
        case Some(v) => params.put("resumptionToken", v)
        case None => {
          logger.debug("End of the line pal, harvest is done.")
          break
        }
      }

      for (doc <- new OaiHarvester(xml)) {
        builder.set("data", doc)
        writer.append(builder.build())
      }
    } }


    writer.close()
  }

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
}
