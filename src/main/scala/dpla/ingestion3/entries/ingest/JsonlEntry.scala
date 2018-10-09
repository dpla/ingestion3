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
  *
  * Usage
  * -----
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.JsonlEntry
  *       --input=/input/path/to/enriched/
  *       --output=/output/path/to/jsonl/
  *       --name=shortName"
  */
object JsonlEntry extends JsonlExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput()
    val dataOut = cmdArgs.getOutput()
    val shortName = cmdArgs.getProviderName()

    val sparkConf =
      new SparkConf()
      .setAppName("jsonl")
      .setMaster("local[*]")

    executeJsonl(sparkConf, dataIn, dataOut, shortName, Utils.createLogger("jsonl"))
  }
}
