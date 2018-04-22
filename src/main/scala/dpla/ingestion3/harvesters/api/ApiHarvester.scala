package dpla.ingestion3.harvesters.api

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{Harvester, UrlBuilder}
import org.apache.log4j.Logger

/**
  * File base harvester
  *
  * @param shortName Provider short name
  * @param conf      Configurations
  * @param outputDir Output path
  * @param logger    Logger
  */
abstract class ApiHarvester(shortName: String,
                            conf: i3Conf,
                            outputDir: String,
                            logger: Logger)
  extends Harvester(shortName, conf, outputDir, logger)
    with UrlBuilder {

  // Abstract method queryParams should set base query parameters for API call.
  protected val queryParams: Map[String, String]

  override protected val filename: String = s"${shortName}_${System.currentTimeMillis()}" +
    ".avro"
}
