package la.dp.ingestion3

import java.io.File
import java.util.concurrent.TimeUnit

import com.twitter.finagle.http.path./
import la.dp.ingestion3.harvesters.OaiHarvester
import la.dp.ingestion3.utils.Utils

/**
  * Created by scott on 1/21/17.
  */
object OaiHarvesterMain extends App with OaiHarvester {

  override def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("Args: <OUT>")
      sys.exit(-1)
    }
    val outDir: File = new File(args(0))
    val endpoint = "aggregator.padigital.org/oai"
    val metadataPrefix = "oai_dc"
    val verb = "ListRecords"

    // Harvest all records
    runHarvest(outDir, endpoint, metadataPrefix, verb)
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
                endpoint: String,
                metadataPrefix: String,
                verb: String) = {
    var resumptionToken: String = ""
    val start = System.currentTimeMillis()

    try {
      do {
        resumptionToken = harvest(outDir, resumptionToken, endpoint, metadataPrefix, verb)
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
    * Prints the runtime of a harvest
    *
    * @param runtime Runtime in milliseconds
    * @param recordsHarvested Number of records in the output directory
    */

  def printResults(runtime: Long, recordsHarvested: Long): Unit = {
    val minutes = (TimeUnit.MILLISECONDS.toMinutes(runtime))
    val seconds = (TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime)))

    val runtimeInSeconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime)
    val recordsPerSecond: Long = recordsHarvested/runtimeInSeconds

    println(s"Harvest count: ${recordsHarvested} records harvested")
    println(s"Runtime: ${minutes} minutes $seconds seconds")
    println(s"Throughput: ${recordsPerSecond} records/second")
  }
}
