package dpla.ingestion3.executors

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.file.FileHarvester
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.harvesters.pss.PssHarvester
import dpla.ingestion3.harvesters.resourceSync.RsHarvester
import dpla.ingestion3.utils.ProviderRegistry
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

trait HarvestExecutor {

  /**
    * Run the appropriate type of harvest.
    *
    * @param shortName Provider short name (e.g. cdl, mdl, nara, loc).
    *                  @see ProviderRegistry.register() for the authoritative
    *                       list of provider short names.
    * @param outputDir Location to save output from harvest
    * @param conf Configurations read from application configuration file
    * @param logger Logger object
    * @return Try[Long] The number of successfully harvested records
    */
  def execute(shortName: String,
                     outputDir: String,
                     conf: i3Conf,
                     logger: Logger): Try[Long] = {

    // Log config file location and provider short name.
    logger.info(s"Harvest initiated")
    logger.info(s"Provider short name: $shortName")

    // Get and log harvest type.
    val harvestType = conf.harvest.harvestType
      .getOrElse(throw new RuntimeException("No harvest type specified."))

    logger.info(s"Harvest type: $harvestType")

    val harvester: Harvester = harvestType match {
      case "oai" =>
        new OaiHarvester(shortName, conf, outputDir, logger)
      case "pss" =>
        new PssHarvester(shortName, conf, outputDir, logger)
      case "rs" =>
        new RsHarvester(shortName, conf, outputDir, logger)
      case t if Seq("api", "file").contains(t) =>
        registeredHarvester(shortName, outputDir, conf, logger)
      case _ =>
        val msg = s"Harvest type not recognized."
        logger.fatal(msg)
        throw new RuntimeException(msg)
    }

    harvester.harvest
  }

  /**
    * Look up a registered Harvester class with the given shortName and instantiate.
    *
    * @param shortName Provider short name
    * @param outputDir Path to save harvested records
    * @param conf Configuration properties
    * @param logger Logger
    * @return
    */
  def registeredHarvester(shortName: String,
                          outputDir: String,
                          conf: i3Conf,
                          logger: Logger): Harvester = {

    val harvesterClass = ProviderRegistry.lookupHarvesterClass(shortName) match {
      case Success(harvClass) => harvClass
      case Failure(e) =>
        logger.fatal(e.getMessage)
        throw e
    }

    harvesterClass.getConstructor(classOf[String], classOf[i3Conf], classOf[String], classOf[Logger])
      .newInstance(shortName, conf, outputDir, logger)
  }
}
