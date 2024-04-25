package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.executors.HarvestExecutor
import org.apache.spark.SparkConf

/** Entry point for running a harvest.
  *
  * Expects three command-line args: --output Path to output directory or S3
  * bucket --conf Path to conf file --name Provider short name --sparkMaster
  * optional parameter that overrides a --master param submitted via
  * spark-submit (e.g. local[*])
  */
object HarvestEntry extends HarvestExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataOut = cmdArgs.getOutput
    val confFile = cmdArgs.getConfigFile
    val shortName = cmdArgs.getProviderName
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val providerConf: i3Conf = i3Conf.load()

    val baseConf = new SparkConf()
      .setAppName(s"Harvest: $shortName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None    => baseConf
    }

    // Execute harvest.
    execute(sparkConf, shortName, dataOut, providerConf)
  }
}
