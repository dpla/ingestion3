package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf}
import dpla.ingestion3.executors.{EnrichExecutor, JsonlExecutor, MappingExecutor}
import dpla.ingestion3.entries.reports.ReporterMain._
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf
/**
  * Single entry point to run harvest, mapping, enrichment, indexing jobs and all reports.
  * Support running against an already harvested data set and running a fresh harvest
  *
  * Expects four parameters:
  *   1)  --output  A base path to save the mapping, enrichment, json-l and report outputs
  *   2)  --input   A path to the harvested data
  *   3)  --conf    A path to the application configuration file
  *   4)  --name    Provider short name
  */
object IngestRemap extends MappingExecutor
  with JsonlExecutor
  with EnrichExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val baseDataOut = cmdArgs.getOutput()
    val confFile = cmdArgs.getConfigFile()
    val shortName = cmdArgs.getProviderName()

    // Outputs
    val harvestDataOut = cmdArgs.getInput()
    val mapDataOut = baseDataOut+"/mapped"
    val enrichDataOut = baseDataOut+"/enriched"
    val jsonlDataOut = baseDataOut+"/json-l"
    val baseRptOut = baseDataOut+"/reports"

    // Get logger
    val logger = Utils.createLogger("ingest", shortName)

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val conf = i3Conf.load()

    // Read spark master property from conf, default to 'local[1]' if not set
    val sparkMaster = conf.spark.sparkMaster.getOrElse("local[1]")

    val sparkConf = new SparkConf()
      .setAppName(s"Mapping: $shortName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")
      .setMaster(sparkMaster)

    // TODO These processes should return some flag or metric to help determine whether to proceed
    // Mapping
    logger.info(s"Saving mapping output to: $mapDataOut")
    executeMapping(sparkConf, harvestDataOut, mapDataOut, shortName, logger)

    // Enrichment
    logger.info(s"Saving enrichment output to: $enrichDataOut")
    executeEnrichment(sparkConf, mapDataOut, enrichDataOut, shortName, logger, conf)

    // Json-l
    logger.info(s"Saving JSON-L output to: $jsonlDataOut")
    executeJsonl(sparkConf, enrichDataOut, jsonlDataOut, logger)

    // Reports
    executeAllReports(sparkConf, mapDataOut, baseRptOut, logger)

    logger.info("Ingest remapping complete")
  }
}