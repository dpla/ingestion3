package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.executors.MappingExecutor
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf


/**
  * Expects four parameters:
  * 1) a path to the harvested data
  * 2) a path to output the mapped data
  * 3) a path to the configuration file
  * 4) provider short name (e.g. 'mdl', 'cdl', 'harvard')
  *
  * Usage
  * -----
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.MappingEntry
  *       --input=/input/path/to/harvested/
  *       --output=/output/path/to/mapped/
  *       --conf=/path/to/conf
  *       --name=shortName"
  */

object MappingEntry extends MappingExecutor {

  def main(args: Array[String]): Unit = {
    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput()
    val dataOut = cmdArgs.getOutput()
    val confFile = cmdArgs.getConfigFile()
    val shortName = cmdArgs.getProviderName()

    // Get mapping logger.
    val logger = Utils.createLogger("mapping", shortName)

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val conf: i3Conf = i3Conf.load()

    // Read spark master property from conf, default to 'local[1]' if not set
    val sparkMaster = conf.spark.sparkMaster.getOrElse("local[1]")

    val sparkConf = new SparkConf()
      .setAppName(s"Mapping: $shortName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .setMaster(sparkMaster)

    // Log config file location and provider short name.
    executeMapping(sparkConf, dataIn, dataOut, shortName, logger)
  }
}
