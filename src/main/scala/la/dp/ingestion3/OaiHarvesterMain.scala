package la.dp.ingestion3

import java.io.File
import java.net.URL
import java.util.concurrent.TimeUnit

import la.dp.ingestion3.harvesters.OaiHarvester
import la.dp.ingestion3.utils.{FileIO, Utils}

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

    if (args.length != 4) {
      println("Bad Args: <OUT>, <OAI URL>, <METADATA PREFIX>, <OAI VERB>")
      sys.exit(-1)
    }
    val outDir: File = new File(args(0))
    val endpoint = new URL(args(1))
    val metadataPrefix = args(2)
    val verb = args(3)

    logger.info(s"Saving records to ${outDir}")
    // Harvest all records
    // runHarvest(outDir, endpoint, metadataPrefix, verb)
  }


  /**
    * Execute the harvest
    *
    * @param outDir Location to save files
    * @param endpoint OAI endpoint
    * @param metadataPrefix
    * @param verb OAI verb
    */
  def runHarvest(outDir: File,
                endpoint: URL,
                metadataPrefix: String,
                verb: String) = {

    var resumptionToken: String = ""
    val start = System.currentTimeMillis()
    val harvester: OaiHarvester = new OaiHarvester(endpoint, metadataPrefix, outDir)

    try {
      do {
        resumptionToken = harvester.harvest(resumptionToken, verb)
      } while (resumptionToken.nonEmpty)

      val end = System.currentTimeMillis()
      val recordsHarvested = Utils.countFiles(outDir, ".xml")
      val runtimeMs = end - start

      printResults(runtimeMs, recordsHarvested)

    } catch {
      case e: Exception => {
        println(e.toString)
      }
    } finally {
      // do nothing...for now
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
    * @param recordsHarvested Number of records in the output directory
    */

  def printResults(runtime: Long, recordsHarvested: Long): Unit = {
    // Make things pretty
    val formatter = java.text.NumberFormat.getIntegerInstance
    val minutes = (TimeUnit.MILLISECONDS.toMinutes(runtime))
    val seconds = (TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime)))
    val runtimeInSeconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime)
    val recordsPerSecond: Long = recordsHarvested/runtimeInSeconds

    println(s"File count: ${formatter.format(recordsHarvested)}")
    println(s"Runtime: ${minutes}:${seconds}")
    println(s"Throughput: ${formatter.format(recordsPerSecond)} records/second")
  }
}
