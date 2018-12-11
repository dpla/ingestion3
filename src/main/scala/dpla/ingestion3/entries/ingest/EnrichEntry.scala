package dpla.ingestion3.entries.ingest

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
  *   5) spark master (optional parameter that overrides a --master param submitted
  *      via spark-submit
  *
  *   Usage
  *   -----
  *   To invoke via sbt:
  *     sbt "run-main dpla.ingestion3.EnrichEntry
  *     --input=/input/path/to/mapped.avro
  *     --output=/output/path/to/enriched.avro
  *     --conf=/path/to/application.conf
  *     --name=provider
  *     --sparkMaster=local[*]
  */

object EnrichEntry extends EnrichExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput()
    val dataOut = cmdArgs.getOutput()
    val confFile = cmdArgs.getConfigFile()
    val shortName = cmdArgs.getProviderName()
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster()

    // Create enrichment logger.
    val enrichLogger = Utils.createLogger("enrichment", shortName)

    // Load configuration from file
    val i3Conf: i3Conf = new Ingestion3Conf(confFile).load()

    val baseConf = new SparkConf()
      .setAppName(s"Enrichment: $shortName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None => baseConf
    }

    executeEnrichment(sparkConf, dataIn, dataOut, shortName, enrichLogger, i3Conf)
  }
}
