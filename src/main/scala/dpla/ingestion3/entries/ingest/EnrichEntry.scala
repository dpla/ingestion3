package dpla.ingestion3.entries.ingest

import java.io.File

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.executors.EnrichExecutor
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf

/**
  * Expects four parameters:
  *   1) a path to the harvested data
  *   2) a path to output the mapped data
  *   3) a path to the application configuration file
  *   4) provider short name
  *
  *   Usage
  *   -----
  *   To invoke via sbt:
  *     sbt "run-main dpla.ingestion3.EnrichEntry
  *     --input=/input/path/to/mapped.avro
  *     --output=/output/path/to/enriched.avro
  *     --conf=/path/to/application.conf
  *     --name=provider
  *
  */

object EnrichEntry extends EnrichExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput()
    val dataOut = cmdArgs.getOutput()
    val confFile = cmdArgs.getConfigFile()
    val shortName = cmdArgs.getProviderName()

    // Create enrichment logger.
    val enrichLogger = Utils.createLogger("enrichment", shortName)

    // Load configuration from file
    val i3Conf: i3Conf = new Ingestion3Conf(confFile).load()

    val sparkConf = new SparkConf()
      .setAppName("Enrichment")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")
      .setMaster(i3Conf.spark.sparkMaster.getOrElse("local[*]"))

    Utils.deleteRecursively(new File(dataOut))

    executeEnrichment(sparkConf, dataIn, dataOut, shortName, enrichLogger, i3Conf)
  }
}
