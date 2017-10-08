package dpla.ingestion3

import dpla.ingestion3.utils.{ProviderRegistry, Utils}
import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.harvesters.pss.PssHarvester
import org.apache.log4j.{LogManager, Logger}

import scala.util.{Failure, Success}

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
    harvestLogger.info(s"Config file: $confFile")
    harvestLogger.info(s"Provider short name: $shortName")

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val providerConf: i3Conf = i3Conf.load()

    // Get and log harvest type.
    val harvestType = providerConf.harvest.harvestType
      .getOrElse(throw new RuntimeException("No harvest type specified."))

    harvestLogger.info(s"Harvest type: $harvestType")

    // Execute harvest.
    executeHarvest(harvestType, shortName, outputDir, providerConf, harvestLogger)
  }

  /**
    * Run the appropriate type of harvest.
    *
    * @param harvestType Abbreviation indicating the type of harvest to run.
    *                    Valid values: [oai, api, pss]
    * @param shortName Provider short name (e.g. cdl, mdl, nara, loc).
    *                  @see ProviderRegistry.register() for the authoritative
    *                       list of provider short names.
    * @param outputDir Location to save output from harvest
    * @param conf Configurations read from application configuration file
    * @param harvestLogger Logger object
    */
  def executeHarvest(harvestType: String,
                     shortName: String,
                     outputDir: String,
                     conf: i3Conf,
                     harvestLogger: Logger) = {

    // TODO Add resource sync type
    val harvester: Harvester = harvestType match {
      case "oai" =>
        new OaiHarvester(shortName, conf, outputDir, harvestLogger)
      case "api" =>
        registeredHarvester(shortName, outputDir, conf, harvestLogger)
      case "pss" =>
        new PssHarvester(shortName, conf, outputDir, harvestLogger)
      case _ =>
        val msg = s"Harvest type not recognized."
        harvestLogger.fatal(msg)
        throw new RuntimeException(msg)
    }

    harvester.harvest
  }

  // Look up a registered Harvester class with the given shortName and instantiate.
  def registeredHarvester(shortName: String,
                          outputDir: String,
                          conf: i3Conf,
                          harvestLogger: Logger): Harvester = {

    val harvesterClass = ProviderRegistry.lookupHarvesterClass(shortName) match {
      case Success(harvClass) => harvClass
      case Failure(e) =>
        harvestLogger.fatal(e.getMessage)
        throw e
    }

    harvesterClass.getConstructor(classOf[String], classOf[i3Conf], classOf[String], classOf[Logger])
      .newInstance(shortName, conf, outputDir, harvestLogger)
  }
}
