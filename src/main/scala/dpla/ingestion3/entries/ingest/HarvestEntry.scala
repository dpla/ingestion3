package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.harvesters.pss.PssHarvester
import dpla.ingestion3.harvesters.resourceSync.RsHarvester
import dpla.ingestion3.utils.{ProviderRegistry, Utils}
import org.apache.log4j.Logger

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

    val dataOut = cmdArgs.getOutput()
    val confFile = cmdArgs.getConfigFile()
    val shortName = cmdArgs.getProviderName()

    // Get mapping logger.
    val harvestLogger = Utils.createLogger("harvest", shortName)

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val providerConf: i3Conf = i3Conf.load()

    // Execute harvest.
    executeHarvest(shortName, dataOut, providerConf, harvestLogger)
  }

  /**
    * Run the appropriate type of harvest.
    *
    * @param shortName Provider short name (e.g. cdl, mdl, nara, loc).
    *                  @see ProviderRegistry.register() for the authoritative
    *                       list of provider short names.
    * @param outputDir Location to save output from harvest
    * @param conf Configurations read from application configuration file
    * @param harvestLogger Logger object
    */
  def executeHarvest(shortName: String,
                     outputDir: String,
                     conf: i3Conf,
                     harvestLogger: Logger) = {

    // Log config file location and provider short name.
    harvestLogger.info(s"Harvest initiated")
    harvestLogger.info(s"Provider short name: $shortName")

    // Get and log harvest type.
    val harvestType = conf.harvest.harvestType
      .getOrElse(throw new RuntimeException("No harvest type specified."))

    harvestLogger.info(s"Harvest type: $harvestType")

    val harvester: Harvester = harvestType match {
      case "oai" =>
        new OaiHarvester(shortName, conf, outputDir, harvestLogger)
      case "api" =>
        registeredHarvester(shortName, outputDir, conf, harvestLogger)
      case "pss" =>
        new PssHarvester(shortName, conf, outputDir, harvestLogger)
      case "rs" =>
        new RsHarvester(shortName, conf, outputDir, harvestLogger)
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
