package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.executors.HarvestExecutor
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf

/**
  * Entry point for running a harvest.
  *
  * Expects three command-line args:
  *   --output  Path to output directory or S3 bucket
  *   --conf    Path to conf file
  *   --name    Provider short name
  */
object HarvestEntry extends HarvestExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataOut = cmdArgs.getOutput()
    val confFile = cmdArgs.getConfigFile()
    val shortName = cmdArgs.getProviderName()

    // Get mapping logger.
    val harvestLogger = Utils.createLogger("harvest", shortName)

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val providerConf: i3Conf = i3Conf.load()

    val sparkConf = new SparkConf()
      .setAppName("Harvest")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")
      .setMaster(providerConf.spark.sparkMaster.getOrElse("local[*]"))

    // Execute harvest.
    execute(sparkConf, shortName, dataOut, providerConf, harvestLogger)
  }
}
