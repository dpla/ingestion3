package dpla.ingestion3.executors

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.file.FileHarvester
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.harvesters.pss.PssHarvester
import dpla.ingestion3.harvesters.resourceSync.RsHarvester
import dpla.ingestion3.utils.ProviderRegistry
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

trait HarvestExecutor {

  /**
    * Run the appropriate type of harvest.
    *
    * @param shortName Provider short name (e.g. cdl, mdl, nara, loc).
    * @see ProviderRegistry.register() for the authoritative
    *      list of provider short names.
    * @param outputDir Location to save output from harvest
    * @param conf      Configurations read from application configuration file
    * @param logger    Logger object
    * @return Try[Long] The number of successfully harvested records
    */
  def execute(sparkConf: SparkConf,
              shortName: String,
              outputDir: String,
              conf: i3Conf,
              logger: Logger): Try[Long] = {

    // Log config file location and provider short name.
    logger.info(s"Harvest initiated")
    logger.info(s"Provider short name: $shortName")

    //todo build spark here
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Get and log harvest type.
    val harvestType = conf.harvest.harvestType
      .getOrElse(throw new RuntimeException("No harvest type specified."))
    logger.info(s"Harvest type: $harvestType")
    val harvester = buildHarvester(spark, shortName, outputDir, conf, logger, harvestType)
    val result = harvester.harvest
    spark.stop()
    result
  }

  private def buildHarvester(spark: SparkSession, shortName: String, outputDir: String, conf: i3Conf, logger: Logger, harvestType: String) = {
    harvestType match {
      case "oai" =>
        new OaiHarvester(spark, shortName, conf, outputDir, logger)
      case "pss" =>
        new PssHarvester(spark, shortName, conf, outputDir, logger)
      case "rs" =>
        new RsHarvester(spark, shortName, conf, outputDir, logger)
      case "api" | "file" =>
        val harvesterClass = ProviderRegistry.lookupHarvesterClass(shortName) match {
          case Success(harvClass) => harvClass
          case Failure(e) =>
            logger.fatal(e.getMessage)
            throw e
        }
        harvesterClass
          .getConstructor(classOf[SparkSession], classOf[String], classOf[i3Conf], classOf[String], classOf[Logger])
          .newInstance(spark, shortName, conf, outputDir, logger)

      case _ =>
        val msg = s"Harvest type not recognized."
        logger.fatal(msg)
        throw new RuntimeException(msg)
    }
  }
}
