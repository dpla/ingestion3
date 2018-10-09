package dpla.ingestion3.confs

import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
  *
  * @param confFilePath Required for all operations (harvest, mapping,
  *                     or enrichment)
  * @param providerName Optional - Provider shortName used to lookup provider
  *                     specific settings in application configuration file.
  *
  *                     Harvest operations require a set of provider settings
  *
  */
class Ingestion3Conf(confFilePath: String, providerName: Option[String] = None) extends ConfUtils {
  def load(): i3Conf = {
    ConfigFactory.invalidateCaches()

    if (confFilePath.isEmpty) throw new IllegalArgumentException("Missing path to conf file")

    val confString = getConfigContents(confFilePath)

    val baseConfig = ConfigFactory.parseString(confString.getOrElse(
      throw new RuntimeException(s"Unable to load configuration file at $confFilePath")))

    val providerConf = providerName match {
      case Some(name) => baseConfig.getConfig(name)
        .withFallback(baseConfig)
        .resolve()
      case _ => baseConfig.resolve()
    }

    i3Conf(
      provider = getProp(providerConf, "provider"),
      Harvest(
        // Generally applicable to all harvesters
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
        // FIXME these should be removed
        sparkDriverMemory = getProp(providerConf, "spark.driverMemory"),
        sparkExecutorMemory= getProp(providerConf, "spark.executorMemory")
      ),
      i3Twofishes(
        hostname = getProp(providerConf, "twofishes.hostname"),
        port = getProp(providerConf, "twofishes.port")
      )
    )
  }
}

/**
  * Command line arguments
  *
  * @param arguments
  */
class CmdArgs(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input: ScallopOption[String] = opt[String](
    "input",
    required = false,
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
    required = false,
    noshort = true,
    validate = _.endsWith(".conf"),
    descr = "Configuration file must end with .conf"
  )

  val providerName: ScallopOption[String] = opt[String](
    "name",
    required = true,
    noshort = true,
    validate = _.nonEmpty
  )

  /**
    * Gets the configuration file property from command line arguments
    *
    * @return Configuration file location
    */
  def getConfigFile() = configFile.toOption
    .map(_.toString)
    .getOrElse(throw new RuntimeException("No configuration file specified."))

  /**
    * Gets the input property from command line arguments
    *
    * @return Input location
    */
  def getInput() = input.toOption
    .map(_.toString)
    .getOrElse(throw new RuntimeException("No input specified."))

  /**
    * Gets the output property from command line arguments
    *
    * @return Output location
    */
  def getOutput() = output.toOption
    .map(_.toString)
    .getOrElse(throw new RuntimeException("No output specified."))

  /**
    * Gets the provider short name from command line arguments
    *
    * @return Provider short name
    */
  def getProviderName() = providerName.toOption
    .map(_.toString)
    .getOrElse(throw new RuntimeException("No provider name specified."))

  verify()
}

/**
  * Classes for defining the application.conf file
  */
case class Harvest (
                      // General
                      endpoint: Option[String] = None,
                      setlist: Option[String] = None,
                      blacklist: Option[String] = None,
                      harvestType: Option[String] = None,
                      // OAI
                      verb: Option[String] = None,
                      metadataPrefix: Option[String] = None,
                      harvestAllSets: Option[String] = None,
                      // API
                      rows: Option[String] = None,
                      query: Option[String] = None,
                      apiKey: Option[String] = None
                    )

case class i3Conf(
                   provider: Option[String] = None,
                   harvest: Harvest = Harvest(),
                   spark: i3Spark = i3Spark(),
                   twofishes: i3Twofishes = i3Twofishes()
                 )

case class i3Twofishes(
                  hostname: Option[String] = None,
                  port: Option[String] = None
                )

case class i3Spark (
                     sparkMaster: Option[String] = None,
                     sparkDriverMemory: Option[String] = None,
                     sparkExecutorMemory: Option[String] = None
                   )