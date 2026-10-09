package dpla.ingestion3.entries.ingest

import com.typesafe.config.ConfigFactory
import dpla.ingestion3.confs.{ConfUtils, Ingestion3Conf, i3Conf}
import dpla.ingestion3.harvesters.api.{DiscoveryResult, PrimoIdDiscovery}
import dpla.ingestion3.harvesters.api.PrimoIdDiscovery.messageOf
import org.apache.logging.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Entry point for Primo `newrecords` id discovery.
  *
  * {{{
  *   --conf    path to i3.conf
  *   --output  data root (ids are written to <root>/<hub>/discovery/)
  *   --name    hub short name, or `all` for every hub with discovery enabled
  * }}}
  *
  * Runs outside Spark on purpose: this makes a handful of HTTP requests and
  * writes a text file, and a SparkSession would cost more to start than the work
  * costs to do.
  *
  * Exit status is the contract the scheduler reads -- `1` means at least one hub
  * failed or came back incomplete, so the run is never quietly half-done. Ids
  * found before a failure are still written; losing them would defeat the point.
  */
object DiscoverIdsEntry extends ConfUtils {

  private val logger = LogManager.getLogger(this.getClass)

  val AllHubs = "all"

  def main(args: Array[String]): Unit = {
    val cmdArgs = new DiscoverCmdArgs(args)
    val confFile = cmdArgs.getConfigFile
    val dataRoot = cmdArgs.getOutput
    val requested = cmdArgs.getProviderName

    // `--ifEnabled` is for callers that name a hub without knowing whether it is
    // on this harvest path -- a full harvest banking fresh ids before it reads
    // its seed, say. Without it, naming a disabled hub is an error, because
    // doing so by hand is nearly always a mistake.
    val tolerateDisabled = cmdArgs.getIfEnabled

    val hubs =
      if (requested.equalsIgnoreCase(AllHubs)) enabledHubs(confFile)
      else Seq(requested)

    if (hubs.isEmpty) {
      logger.warn(
        s"No hubs have harvest.discovery.enabled = true in $confFile. Nothing to do."
      )
      return
    }

    logger.info(s"Discovery for ${hubs.size} hub(s): ${hubs.mkString(", ")}")

    // Strictness belongs to "the operator named this hub", not to "the list came
    // back with one entry in it" -- an `all` run that resolves to a single hub is
    // still an `all` run.
    val named = !requested.equalsIgnoreCase(AllHubs)
    val outcomes = hubs.map(hub =>
      hub -> runOne(hub, confFile, dataRoot, explicit = named && !tolerateDisabled)
    )

    val failures = outcomes.collect {
      case (hub, Failure(e))                        => s"$hub: ${messageOf(e)}"
      case (hub, Success(Some(r))) if !r.complete   => s"$hub: ${r.shortfalls.mkString("; ")}"
    }

    logger.info("=" * 78)
    outcomes.foreach {
      case (_, Success(Some(r))) => logger.info(r.summary)
      case (hub, Success(None))  => logger.info(s"$hub discovery: skipped (not enabled)")
      case (hub, Failure(e))     => logger.error(s"$hub discovery: FAILED -- ${messageOf(e)}")
    }
    logger.info("=" * 78)

    if (failures.nonEmpty) {
      logger.error(s"${failures.size} hub(s) did not complete:")
      failures.foreach(f => logger.error(s"  $f"))
      // The scheduler alerts on a non-zero exit; a partial discovery that
      // reported success would let ids fall out of the window unnoticed, which
      // is the exact failure this job exists to prevent.
      System.exit(1)
    }
  }

  /** Runs one hub. `None` means the hub is configured but switched off. */
  private[ingest] def runOne(
      hub: String,
      confFile: String,
      dataRoot: String,
      explicit: Boolean
  ): Try[Option[DiscoveryResult]] = Try {
    val providerConf: i3Conf = new Ingestion3Conf(confFile, Some(hub)).load()

    if (!isEnabled(providerConf.harvest.discovery.enabled)) {
      // Naming a disabled hub is almost always a mistake worth surfacing;
      // skipping it inside an `all` run is routine.
      if (explicit)
        throw new RuntimeException(
          s"$hub has harvest.discovery.enabled = false (or unset). Enable it in " +
            s"i3.conf, or run a hub that is enabled."
        )
      logger.info(s"$hub: discovery not enabled; skipping")
      None
    } else {
      Some(new PrimoIdDiscovery(hub, providerConf, dataRoot).run())
    }
  }

  /** Top-level hub keys with `harvest.discovery.enabled = true`.
    *
    * Read straight from the parsed conf rather than by pattern-matching the file
    * text, so a hub cannot be missed because of formatting.
    */
  private[ingest] def enabledHubs(confFile: String): Seq[String] = {
    ConfigFactory.invalidateCaches()
    val contents = getConfigContents(confFile).getOrElse(
      throw new RuntimeException(s"Unable to load configuration file at $confFile")
    )
    val config = ConfigFactory.parseString(contents)
    config
      .root()
      .keySet()
      .asScala
      .toSeq
      .sorted
      .filter { hub =>
        Try {
          val path = s"$hub.harvest.discovery.enabled"
          config.hasPath(path) && isEnabled(Some(config.getString(path)))
        }.getOrElse(false)
      }
  }

  /** HOCON permits `true`, `"true"`, `yes` and `on`; treat anything else as off
    * so a typo disables a hub loudly rather than enabling it silently.
    */
  private[ingest] def isEnabled(raw: Option[String]): Boolean =
    raw.map(_.trim.toLowerCase).exists(Set("true", "yes", "on", "1"))
}

private class DiscoverCmdArgs(arguments: Seq[String]) extends ScallopConf(arguments) {
  val output: ScallopOption[String] =
    opt[String]("output", required = true, noshort = true, validate = _.nonEmpty)

  private val configFile: ScallopOption[String] = opt[String](
    "conf",
    required = true,
    noshort = true,
    validate = _.endsWith(".conf"),
    descr = "Configuration file must end with .conf"
  )

  val providerName: ScallopOption[String] = opt[String](
    "name",
    required = true,
    noshort = true,
    validate = _.nonEmpty,
    descr = "Hub short name, or `all` for every hub with discovery enabled"
  )

  private val ifEnabled: ScallopOption[Boolean] = toggle(
    "ifEnabled",
    default = Some(false),
    noshort = true,
    descrYes = "Skip a named hub that has discovery disabled instead of failing"
  )

  def getConfigFile: String = configFile.toOption
    .getOrElse(throw new RuntimeException("No configuration file specified."))

  def getOutput: String = output.toOption
    .getOrElse(throw new RuntimeException("No output specified."))

  def getProviderName: String = providerName.toOption
    .getOrElse(throw new RuntimeException("No provider name specified."))

  def getIfEnabled: Boolean = ifEnabled.toOption.getOrElse(false)

  verify()
}
