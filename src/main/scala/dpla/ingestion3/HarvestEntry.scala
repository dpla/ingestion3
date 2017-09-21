package dpla.ingestion3

import dpla.ingestion3.utils.Utils
import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.harvesters.api.{CdlHarvester, MdlHarvester}
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.harvesters.pss.PssHarvester
import org.apache.log4j.{LogManager, Logger}

/**
  * Entry point for running a harvest.
  *
  * Expects three command-line args:
  *   --output  Path to output directory
  *   --conf    Path to conf file
  *   --name    Provider short name
  */
object HarvestEntry {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val outputDir = cmdArgs.output.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No output specified."))
    val confFile = cmdArgs.configFile.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No conf file specified."))
    val shortName = cmdArgs.providerName.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No provider short name specified."))

    // Get logger.
    val harvestLogger: Logger = LogManager.getLogger("ingestion3")
    val appender = Utils.getFileAppender(shortName, "harvest")
    harvestLogger.addAppender(appender)

    // Log config file location and provider short name.
    harvestLogger.info(s"Harvest initiated")
    harvestLogger.info(s"Config file: ${confFile}")
    harvestLogger.info(s"Provider short name: ${shortName}")

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val providerConf: i3Conf = i3Conf.load()

    // Get and log harvest type.
    val harvestType = providerConf.harvest.harvestType
      .getOrElse(throw new RuntimeException("No harvest type specified."))

    harvestLogger.info(s"Harvest type: ${harvestType}")

    // Execute harvest.
    executeHarvest(harvestType, shortName, outputDir, providerConf, harvestLogger)
  }

  /**
    * Run the appropriate type of harvest.
    *
    * @param harvestType
    * @param shortName
    * @param outputDir
    * @param conf
    * @param harvestLogger
    */
  def executeHarvest(harvestType: String,
                     shortName: String,
                     outputDir: String,
                     conf: i3Conf,
                     harvestLogger: Logger) = {

    // TODO Add resource sync type
    harvestType match {
      case "oai" =>
        new OaiHarvester(shortName, conf, outputDir, harvestLogger).harvest
      case "api" => shortName match {
        case "cdl" =>
          new CdlHarvester(shortName, conf, outputDir, harvestLogger).harvest
        case "mdl" =>
          new MdlHarvester(shortName, conf, outputDir, harvestLogger).harvest
      }
      case "pss" =>
        new PssHarvester(shortName, conf, outputDir, harvestLogger).harvest
      case _ =>
        throw new RuntimeException("Harvest type not recognized.")
    }
  }
}
