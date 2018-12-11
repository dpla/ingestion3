package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.CmdArgs
import dpla.ingestion3.executors.JsonlExecutor
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf

/**
  * Driver for reading DplaMapData records (mapped or enriched) and generating
  * JSONL text, which can be bulk loaded into a DPLA Ingestion1 index, in
  * Elasticsearch 0.90
  *
  * Expects three parameters:
  * 1) a path to the mapped/enriched data
  * 2) a path to output the jsonl data
  * 3) provider short name (e.g. 'mdl', 'cdl', 'harvard')
  * 4) spark master (optional parameter that overrides a --master param submitted
  *    via spark-submit
  * 5) boolean indicating whether or not output should be consolidated into
  *    a single file (defaults to false)
  *
  * Usage
  * -----
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.JsonlEntry
  *       --input=/input/path/to/enriched/
  *       --output=/output/path/to/jsonl/
  *       --name=shortName"
  *       --sparkMaster=local[*]
  *       --mergeOutput=true
  */
object JsonlEntry extends JsonlExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput()
    val dataOut = cmdArgs.getOutput()
    val shortName = cmdArgs.getProviderName()
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster()
    val mergeOutput: Boolean = cmdArgs.getMergeOutput()

    val baseConf =
      new SparkConf()
        .setAppName(s"JSONL: $shortName")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None => baseConf
    }

    val logger = Utils.createLogger("jsonl")

    executeJsonl(sparkConf, dataIn, dataOut, shortName, mergeOutput, logger)
  }
}
