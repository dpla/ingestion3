package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.entries.reports.ReporterMain.executeAllReports
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf

/** Driver for reading DplaMapData records (mapped or enriched) and generating
  * Reports
  *
  * Expects three parameters: 1) a path to the mapped/enriched data 2) a path to
  * output the reports 3) a path to the application configuration file 4)
  * provider short name 5) spark master (optional parameter that overrides a
  * --master param submitted via spark-submit
  *
  * Usage ----- To invoke via sbt: sbt "run-main dpla.ingestion3.ReportsEntry
  * --input=/input/path/to/enriched/ --output=/output/path/to/reports/
  * --conf=/path/to/application.conf --name=shortName" --sparkMaster=local[*]
  */
object ReportsEntry {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput
    val dataOut = cmdArgs.getOutput
    val shortName = cmdArgs.getProviderName
    val confFile = cmdArgs.getConfigFile
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster

    // Load configuration from file
    val i3Conf: i3Conf = new Ingestion3Conf(confFile).load()

    // Get logger
    val logger = Utils.createLogger("reports", shortName)

    val baseConf =
      new SparkConf()
        .setAppName(s"Reports: $shortName")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None    => baseConf
    }

    executeAllReports(sparkConf, dataIn, dataOut, shortName, logger)
  }
}
