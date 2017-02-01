package la.dp.ingestion3

import java.io.File
import java.net.URL
import java.util.concurrent.TimeUnit

import la.dp.ingestion3.harvesters.OaiHarvester
import la.dp.ingestion3.utils.{FlatFileIO, Utils}

import scala.util.control.NonFatal

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
    val endpoint = new URL(args(1))
    val metadataPrefix = args(2)
    val verb = args(3)
    // How to serialize the output
    val fileIO = new FlatFileIO

    logger.debug(s"Saving records to ${outDir}")
    logger.debug(s"Harvesting from ${endpoint}")

    // Create the harvester and run
    val oaiHarvester: OaiHarvester = new OaiHarvester(outDir, fileIO)
    val queryUrlBuilder = new OaiQueryUrlBuilder
    var resumptionToken = ""

    // performance tracking
    val start = System.currentTimeMillis()

    try {
      do {
        val queryUrl = queryUrlBuilder.buildQueryUrl(endpoint, metadataPrefix, resumptionToken, verb)
        resumptionToken = oaiHarvester(queryUrl)
      } while (resumptionToken.nonEmpty)
    } catch {
      case NonFatal(e) => {
        logger.error(e.getMessage)
        System.exit(-1)
      }
    }
    val end = System.currentTimeMillis()

    val recordsHarvestedCount = Utils.countFiles(outDir, ".xml")
    val runtimeMs = (end - start)

    printResults(runtimeMs, recordsHarvestedCount)

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
