package dpla.ingestion3.confs

import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, ScallopOption}

class Ingestion3Conf(confFilePath: String, providerName: String) extends ConfUtils {
  def load(): i3Conf = {
    // Loads config file for more information about loading hierarchy please read
    // https://github.com/typesafehub/config#standard-behavior
    ConfigFactory.invalidateCaches()

    if (confFilePath.isEmpty) throw new IllegalArgumentException("Missing path to conf file")

    System.setProperty(getConfFileLocation(confFilePath), confFilePath)

    val baseConfig = ConfigFactory.load

    val providerConf = ConfigFactory
      .load
      .getConfig(providerName)
      .withFallback(baseConfig)
      .resolve()

    i3Conf(
      provider = getProp(providerConf, "provider") match {
        case Some(d) => Some(d)
        case None => throw new IllegalArgumentException("Provider is not " +
          "specified in config. Cannot harvest.")
      },
      Harvest(
        // Generally applicable
        endpoint = getProp(providerConf, "harvest.endpoint"),
        setlist = getProp(providerConf, "harvest.setlist"),
        blacklist = getProp(providerConf, "harvest.blacklist"),
        harvestType = getProp(providerConf, "harvest.type"),
        // Properties for OAI harvests
        verb = getProp(providerConf, "harvest.verb"),
        metadataPrefix = getProp(providerConf, "harvest.metadataPrefix"),
        harvestAllSets = getProp(providerConf, "harvest.harvestAllSets"),
        // Properties for API harvests
        apiKey = getProp(providerConf, "harvest.apiKey"),
        rows = getProp(providerConf, "harvest.rows"),
        query = getProp(providerConf, "harvest.query")
      ),
      i3Spark(
        sparkMaster = getProp(providerConf, "spark.master"),
        sparkDriverMemory = getProp(providerConf, "spark.driverMemory"),
        sparkExecutorMemory= getProp(providerConf, "spark.executorMemory")
      )
    )
  }
}


/**
  * Command line arguments for invoking a harvester
  *
  * @param arguments
  */
class HarvestCmdArgs(arguments: Seq[String]) extends ScallopConf(arguments) {
  val providerName: ScallopOption[String] = opt[String](
    "name",
    required = true,
    noshort = true,
    validate = _.nonEmpty
  )

  val output: ScallopOption[String] = opt[String](
    "output",
    required = true,
    noshort = true,
    validate = _.nonEmpty
  )

  val configFile: ScallopOption[String] = opt[String](
    "conf",
    required = true,
    noshort = true,
    validate = _.endsWith(".conf"),
    descr = "Configuration file must end with .conf"
  )

  verify()
}


/**
  * Classes for defining the application.conf file
  */
case class Harvest (
                      // General
                      endpoint: Option[String],
                      setlist: Option[String],
                      blacklist: Option[String],
                      harvestType: Option[String],
                      // OAI
                      verb: Option[String],
                      metadataPrefix: Option[String],
                      harvestAllSets: Option[String],
                      // API
                      rows: Option[String],
                      query: Option[String],
                      apiKey: Option[String]
                    )

case class i3Conf(
                   provider: Option[String],
                   harvest: Harvest,
                   spark: i3Spark
                 )

case class i3Spark (
                     sparkMaster: Option[String],
                     sparkDriverMemory: Option[String],
                     sparkExecutorMemory: Option[String]
                   )