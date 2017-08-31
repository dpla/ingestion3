package dpla.ingestion3.confs

import com.typesafe.config.ConfigFactory

/**
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

class OaiHarvesterConf(arguments: Seq[String]) {
  def load(): OaiConf = {
    // Loads config file for more information about loading hierarchy please read
    // https://github.com/typesafehub/config#standard-behavior
    ConfigFactory.invalidateCaches()
    val oaiConf = ConfigFactory.load()

    def getProp(prop: String, default: Option[String] = None): Option[String] = {
      oaiConf.hasPath(prop) match {
        case true => Some(oaiConf.getString(prop))
        case false => default
      }
    }

    OaiConf(
      // Validate outputDir parameter here since it is not validated in DefaultSource
      outputDir = getProp("outputDir") match {
        case Some(d) => Some(d)
        case None => throw new IllegalArgumentException("Output directory is not " +
          "specified in config. Cannot harvest.")
      },
      endpoint = getProp("endpoint"),
      verb = getProp("verb"),
      // Validate provider parameter here since it is not validated in DefaultSource
      provider = getProp("provider") match {
        case Some(d) => Some(d)
        case None => throw new IllegalArgumentException("Provider is not " +
          "specified in config. Cannot harvest.")
      },
      metadataPrefix = getProp("metadataPrefix"),
      harvestAllSets = getProp("harvestAllSets"),
      setlist = getProp("setlist"),
      blacklist = getProp("blacklist"),
      // Default spark master is to run on local
      sparkMaster = getProp("sparkMaster", Some("local[*]"))
    )
  }
}

case class OaiConf(
                       outputDir: Option[String],
                       endpoint: Option[String],
                       verb: Option[String],
                       provider: Option[String],
                       metadataPrefix: Option[String],
                       harvestAllSets: Option[String],
                       setlist: Option[String],
                       blacklist: Option[String],
                       sparkMaster: Option[String]
                     )