package dpla.ingestion3.confs

import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, ScallopOption}

/** @param confFilePath
  *   Required for all operations (harvest, mapping, or enrichment)
  * @param providerName
  *   Optional - Provider shortName used to lookup provider specific settings in
  *   application configuration file.
  *
  * Harvest operations require a set of provider settings
  */
class Ingestion3Conf(confFilePath: String, providerName: Option[String] = None)
    extends ConfUtils {
  def load(): i3Conf = {
    ConfigFactory.invalidateCaches()

    if (confFilePath.isEmpty)
      throw new IllegalArgumentException("Missing path to conf file")

    val confString = getConfigContents(confFilePath)

    val baseConfig = ConfigFactory.parseString(
      confString.getOrElse(
        throw new RuntimeException(
          s"Unable to load configuration file at $confFilePath"
        )
      )
    )

    val providerConf = providerName match {
      case Some(name) =>
        baseConfig
          .getConfig(name)
          .withFallback(baseConfig)
          .resolve()
      case _ => baseConfig.resolve()
    }

    i3Conf(
      email = getProp(providerConf, "email"),
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
        httpVersion = getProp(providerConf, "harvest.httpVersion"),
        // Properties for API harvests
        apiKey = getProp(providerConf, "harvest.apiKey"),
        rows = getProp(providerConf, "harvest.rows"),
        query = getProp(providerConf, "harvest.query"),
        // Properties for FileDelta harvests
        update = getProp(providerConf, "harvest.delta.update"),
        previous = getProp(providerConf, "harvest.delta.previous"),
        deletes = getProp(providerConf, "harvest.delta.deletes"),
        sleep = getProp(providerConf, "harvest.sleep"),
        awsProfile = getProp(providerConf, "harvest.aws.profile"),
        // Properties for Primo `newrecords` id discovery
        discovery = Discovery(
          enabled = getProp(providerConf, "harvest.discovery.enabled"),
          endpoint = getProp(providerConf, "harvest.discovery.endpoint"),
          apiKey = getProp(providerConf, "harvest.discovery.apiKey"),
          viewParams = getProp(providerConf, "harvest.discovery.viewParams"),
          query = getProp(providerConf, "harvest.discovery.query"),
          newRecordsFacet = getProp(providerConf, "harvest.discovery.newRecordsFacet"),
          windows = getProp(providerConf, "harvest.discovery.windows"),
          maxOffset = getProp(providerConf, "harvest.discovery.maxOffset"),
          pageLimit = getProp(providerConf, "harvest.discovery.pageLimit"),
          partitionField = getProp(providerConf, "harvest.discovery.partitionField"),
          partitionAlphabet = getProp(providerConf, "harvest.discovery.partitionAlphabet"),
          partitionBaseFacets = getProp(providerConf, "harvest.discovery.partitionBaseFacets"),
          maxPartitionDepth = getProp(providerConf, "harvest.discovery.maxPartitionDepth"),
          restSeconds = getProp(providerConf, "harvest.discovery.restSeconds")
        )
      ),
      i3Spark(
        // FIXME these should be removed
        sparkDriverMemory = getProp(providerConf, "spark.driverMemory"),
        sparkExecutorMemory = getProp(providerConf, "spark.executorMemory")
      )
    )
  }
}

/** Command line arguments
  *
  * @param arguments
  *   Command line arguments
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

  private val configFile: ScallopOption[String] = opt[String](
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

  val sparkMaster: ScallopOption[String] = opt[String](
    "sparkMaster",
    required = false,
    noshort = true
  )

  val deleteIds: ScallopOption[String] = opt[String](
    "deleteIds",
    required = false,
    noshort = true,
    validate = _.nonEmpty
  )

  /** Gets the configuration file property from command line arguments
    *
    * @return
    *   Configuration file location
    */
  def getConfigFile: String = configFile.toOption
    .getOrElse(throw new RuntimeException("No configuration file specified."))

  /** Gets the input property from command line arguments
    *
    * @return
    *   Input location
    */
  def getInput: String = input.toOption
    .getOrElse(throw new RuntimeException("No input specified."))

  /** Gets the output property from command line arguments
    *
    * @return
    *   Output location
    */
  def getOutput: String = output.toOption
    .getOrElse(throw new RuntimeException("No output specified."))

  /** Gets the provider short name from command line arguments
    *
    * @return
    *   Provider short name
    */
  def getProviderName: String = providerName.toOption
    .getOrElse(throw new RuntimeException("No provider name specified."))

  def getSparkMaster: Option[String] = sparkMaster.toOption

  def getDeleteIds: Option[String] = deleteIds.toOption

  verify()
}

/** Classes for defining the application.conf file
  */
case class Harvest(
    // General
    endpoint: Option[String] = None,
    setlist: Option[String] = None,
    blacklist: Option[String] = None,
    harvestType: Option[String] = None,
    // OAI
    verb: Option[String] = None,
    metadataPrefix: Option[String] = None,
    harvestAllSets: Option[String] = None,
    // Preferred HTTP version for OAI requests: "1.1" or "2" (default: client
    // default, HTTP/2). Set "1.1" for endpoints that drop long HTTP/2
    // connections with a GOAWAY during a sustained harvest (e.g. Apache
    // mod_http2). See docs/ingestion/dartmouth-mapping-draft.md.
    httpVersion: Option[String] = None,
    // API
    rows: Option[String] = None,
    query: Option[String] = None,
    apiKey: Option[String] = None,
    // File delta
    // Process NARA ingest using a incremental update of records
    update: Option[String] = None, // Path to delta update records
    previous: Option[String] = None, // Path to previously harvested records
    deletes: Option[String] = None, // Path to deletes
    sleep: Option[String] = None,
    awsProfile: Option[String] = None, // Named AWS profile for cross-account S3 access
    // Primo `newrecords` id discovery, run on its own schedule
    discovery: Discovery = Discovery()
)

/** Settings for [[dpla.ingestion3.harvesters.api.PrimoIdDiscovery]].
  *
  * Everything a Primo tenant differs on lives here, so wiring a new hub onto the
  * id-discovery path is a config change and not a code change. Defaults suit
  * Getty; a tenant with a lower paging cap (Mississippi's is `offset + limit <=
  * 500`) must set `maxOffset` and `pageLimit` to match, or discovery will page
  * past the ceiling and fail.
  */
case class Discovery(
    enabled: Option[String] = None,
    endpoint: Option[String] = None,
    apiKey: Option[String] = None, // falls back to harvest.apiKey
    // `vid=DPLA,tab=dpla,scope=DPLA,inst=01GRI,lang=eng`
    viewParams: Option[String] = None,
    query: Option[String] = None, // falls back to harvest.query
    newRecordsFacet: Option[String] = None,
    windows: Option[String] = None,
    maxOffset: Option[String] = None,
    pageLimit: Option[String] = None,
    // Used only when a window is too large to page and must be sliced
    partitionField: Option[String] = None,
    partitionAlphabet: Option[String] = None,
    // Facet clauses restated while partitioning, because the prefix clause
    // displaces the base query out of `q`
    partitionBaseFacets: Option[String] = None,
    maxPartitionDepth: Option[String] = None,
    restSeconds: Option[String] = None
)

case class i3Conf(
    email: Option[String] = None,
    provider: Option[String] = None,
    harvest: Harvest = Harvest(),
    spark: i3Spark = i3Spark()
)

case class i3Spark(
    sparkDriverMemory: Option[String] = None,
    sparkExecutorMemory: Option[String] = None
)
