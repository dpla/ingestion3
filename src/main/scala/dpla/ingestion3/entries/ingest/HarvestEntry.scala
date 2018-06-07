package dpla.ingestion3.entries.ingest

import java.io.File

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.executors.HarvestExecutor
import dpla.ingestion3.utils.{FlatFileIO, Utils}

import scala.util.Failure

/**
  * Entry point for running a harvest.
  *
  * Expects three command-line args:
  *   --output  Path to output directory
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

    // Delete dataOut if exists
    if (new File(dataOut).exists()) {
      harvestLogger.warn(s"$dataOut already exists. Deleting")
      new FlatFileIO().deletePathContents(dataOut)
    }

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val providerConf: i3Conf = i3Conf.load()

    // Execute harvest.
    execute(shortName, dataOut, providerConf, harvestLogger) match {
      case Failure(error) => error.printStackTrace()
      case _ =>
    }
  }
}
