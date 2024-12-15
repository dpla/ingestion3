package dpla.ingestion3.entries.utils

import dpla.ingestion3.confs.CmdArgs
import dpla.ingestion3.dataStorage.InputHelper
import dpla.ingestion3.entries.Entry
import dpla.ingestion3.executors.DeleteExecutor
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf

/** Driver for reading DplaMapData records (mapped or enriched), deleting
  * specific IDs and generating JSONL text, which can be bulk loaded into a DPLA
  * Ingestion1 index, in Elasticsearch 0.90
  *
  * Expects three parameters: 1) a path to the enriched data 2) a path to output
  * the jsonl data 3) provider short name (e.g. 'mdl', 'cdl', 'harvard') 4)
  * spark master (optional parameter that overrides a --master param submitted
  * via spark-submit 5) a comma separated list of DPLA IDs to delete
  *
  * Usage
  * -----
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.entries.utils.DeleteEntry
  * --input=/input/path/to/enriched/
  * --output=/output/path/provider/
  * --deleteIds=1,2,3,4
  * --name=shortName"
  * --sparkMaster=local[*]
  */
object DeleteEntry extends DeleteExecutor {

  def main(args: Array[String]): Unit = {

    Entry.suppressUnsafeWarnings()

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput
    val dataOut = cmdArgs.getOutput
    val shortName = cmdArgs.getProviderName
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster
    val deleteIds: String = cmdArgs.getDeleteIds.getOrElse("")

    // If given input path is enrichment, use it as `enrichedData`.
    // If not, assume that it is a directory containing several enriched sets and
    // get the most recent enrichment from that directory.
    val enrichedData = if (InputHelper.isActivityPath(dataIn)) {
      dataIn
    } else {
      InputHelper
        .mostRecent(dataIn)
        .getOrElse(
          throw new RuntimeException("Unable to load enriched data.")
        )
    }

    val baseConf =
      new SparkConf()
        .setAppName(s"DELETE: $shortName")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None    => baseConf
    }

    executeDelete(
      sparkConf,
      enrichedData,
      dataOut,
      deleteIds,
      shortName
    )
  }
}
