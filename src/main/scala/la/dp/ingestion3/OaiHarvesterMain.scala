package la.dp.ingestion3

import java.io.File
import java.util.concurrent.TimeUnit

import la.dp.ingestion3.harvesters.OaiHarvester
import la.dp.ingestion3.utils.AvroFileIO
import org.apache.log4j.LogManager

import scala.collection.mutable.Map
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

    val logger = LogManager.getLogger("OaiHarvester")

    // Complains about not being typesafe...
    if(args.length != 4 ) {
      logger.error("Bad number of args: <OUTPUT FILE>, <OAI URL>, <METADATA PREFIX>, <OAI VERB>")
      sys.exit(-1)
    }

    val outputFile: File = new File(args(0))
    val queryUrlBuilder = new OaiQueryUrlBuilder

    // Had to use a mutable...can't quite shake this
    val params = Map[String,String]("endpoint" -> args(1),
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

    // Experiemented with DynamicVariable but I don't think that is the
    // answer to my problems with reumptionToken
    val oaiHarvester = new OaiHarvester()
    val fileIO = new AvroFileIO(avrscheme, outputFile)


    logger.debug(s"Saving records to ${outputFile.toString}")
    try {
      // This is stupid and the surrounding try should probably
      // catch the break exception
      breakable {
        // Had to put it in a loop...can't quite shake this, there has to be a better way...
        while(true) {
          // Construct the URL
          val url: java.net.URL= queryUrlBuilder.buildQueryUrl(params.toMap)
          // Get the XML
          val xml = oaiHarvester.getXmlResponse(url)

          // logger.debug(url.toString)

          // Save the records
          for (doc <- new OaiHarvester(xml)) {
            fileIO.writeFile(doc)
          }

          // Get resumptionToken for next request (if available) and shove it back into the
          // mutable params map
          oaiHarvester.getResumptionToken(xml) match {
            case Some(v) => params.put("resumptionToken", v)
            case None => break
          }
        } // end while
      } // end breakable
    } finally {
      fileIO.close
    }


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
