package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.executors.MappingExecutor
import org.apache.spark.SparkConf

/** Expects four parameters: 1) a path to the harvested data 2) a path to output
  * the mapped data 3) a path to the configuration file 4) provider short name
  * (e.g. 'mdl', 'cdl', 'harvard') 5) spark master (optional parameter that
  * overrides a --master param submitted via spark-submit)
  *
  * Usage ----- To invoke via sbt: sbt "run-main dpla.ingestion3.MappingEntry
  * --input=/input/path/to/harvested/ --output=/output/path/to/mapped/
  * --conf=/path/to/conf --name=shortName" --sparkMaster=local[*]
  */

object MappingEntry extends MappingExecutor {

  def main(args: Array[String]): Unit = {
    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput
    val dataOut = cmdArgs.getOutput
    val shortName = cmdArgs.getProviderName
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster

    val baseConf = new SparkConf()
      .setAppName(s"Mapping: $shortName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None    => baseConf
    }

    // Log config file location and provider short name.
    executeMapping(sparkConf, dataIn, dataOut, shortName)
  }
}
