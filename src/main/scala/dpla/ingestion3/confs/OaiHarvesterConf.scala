package dpla.ingestion3.confs

import org.rogach.scallop.{ScallopConf, ScallopOption}


/**
  * https://github.com/scallop/scallop/wiki
  *
  * Arguments to be passed into the OAI-PMH harvester
  *
  * Output directory: String
  * Endpoint: String the OAI URL
  * Verb: String ListRecords || ListSets
  * Provider: String provider name (we need to standardize this).
  * Prefix (Optional): String metadata prefix, eg. oai_dc oai_qdc, mods, MODS21 etc.
  * Harvest all sets: Boolean: true to harvest records from all sets, default is false
  *                   Use with verb = "ListRecords"
  * Setlist (Optional): Comma-separated String of sets to harvest
  *                     Use with verb = "ListRecords"
  * Blacklist (Optional): Comma-separated String of sets to exclude from harvest
  *                       Use with verb = "ListRecords"
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
  val verb: ScallopOption[String] = opt[String]("verb",
    required = true,
    noshort = true,
    validate = (_.nonEmpty))
  val provider: ScallopOption[String] = opt[String]("provider",
    required = true,
    noshort = true)
  // Optional parameters
  val prefix: ScallopOption[String] = opt[String]("prefix",
    required = false,
    noshort = true,
    validate = (_.nonEmpty))
  val harvestAllSets: ScallopOption[String] = opt[String]("harvestAllSets",
    required = false,
    noshort = true,
    default = Some("false"))
  val setlist: ScallopOption[String] = opt[String]("setlist",
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
