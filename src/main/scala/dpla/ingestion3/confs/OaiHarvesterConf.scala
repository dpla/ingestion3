package dpla.ingestion3.confs

import org.rogach.scallop.{ScallopConf, ScallopOption}


/**
  * https://github.com/scallop/scallop/wiki
  *
  * Arguments to be passed into the OAI-PMH harvester
  *
  * Output directory: String
  * OAI URL: String
  * Metadata Prefix: String oai_dc oai_qdc, mods, MODS21 etc.
  * Provider: Provider name (we need to standardize this).
  * Sets (Optional): Comma-separated String of sets to harvest from.
  * Blacklist (Optional): Comma-separated String of sets to exclude from harvest
  * sparkMaster (Optional): If omitted the defaults to local[*]
  */

class OaiHarvesterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  // Required parameters
  val outputDir: ScallopOption[String] = opt[String]("outputDir",
    required = true,
    noshort = true,
    validate = (_.nonEmpty))
  val endpoint: ScallopOption[String] = opt[String]("endpoint",
    required = true,
    noshort = true,
    // TODO is there a better validation of the URL rather than nonEmpty?
    validate = (_.nonEmpty))
  val prefix: ScallopOption[String] = opt[String]("prefix",
    required = true,
    noshort = true,
    validate = (_.nonEmpty))
  val provider: ScallopOption[String] = opt[String]("provider",
    required = true,
    noshort = true,
    validate = (_.nonEmpty))
  // Optional parameters
  val sets: ScallopOption[String] = opt[String]("sets",
    required = false,
    noshort = true,
    validate = (_.nonEmpty))
  val blacklist: ScallopOption[String] = opt[String]("blacklist",
    required = false,
    noshort = true,
    validate = (_.nonEmpty))
  // If a master URL is not provider then default to local[*]
  val sparkMaster: ScallopOption[String] = opt[String]("sparkMaster",
    required = false,
    noshort = true,
    default = Some("local[*]"))

  verify()
}
