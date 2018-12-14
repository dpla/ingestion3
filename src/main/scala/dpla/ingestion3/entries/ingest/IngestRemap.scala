package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf}
import dpla.ingestion3.dataStorage.InputHelper
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
  *   5)  --sparkMaster optional parameter that overrides a --master param submitted
  *                     via spark-submit (e.g. local[*])
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
    val input = cmdArgs.getInput()
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster()

    // Get logger
    val logger = Utils.createLogger("ingest", shortName)

    // Outputs

    // If given input path is a harvest, use it as `harvestData'.
    // If not, assume that it is a directory containing several harvests and
    // get the most recent harvest from that directory.
    val harvestData = InputHelper.isActivityPath(input) match {
      case true => input
      case false => InputHelper.mostRecent(input)
        .getOrElse(throw new RuntimeException("Unable to load harvest data."))
    }

    logger.info(s"Using harvest data from $harvestData")

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val conf = i3Conf.load()

    val baseConf = new SparkConf()
      .setAppName(s"IngestRemap: $shortName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None => baseConf
    }

    // TODO These processes should return some flag or metric to help determine whether to proceed
    // Mapping
    val mapDataOut: String =
    executeMapping(sparkConf, harvestData, baseDataOut, shortName, logger)

    // Enrichment
    val enrichDataOut: String =
      executeEnrichment(sparkConf, mapDataOut, baseDataOut, shortName, logger, conf)

    // Json-l
    executeJsonl(sparkConf, enrichDataOut, baseDataOut, shortName, logger)

    // Reports
    executeAllReports(sparkConf, enrichDataOut, baseDataOut, shortName, logger)
  }
}
