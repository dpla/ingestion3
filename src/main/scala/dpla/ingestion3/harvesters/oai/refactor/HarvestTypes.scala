package dpla.ingestion3.harvesters.oai.refactor

abstract sealed class HarvestTypes

case class BlacklistHarvest(endpoint: String, metadataPrefix: String, blacklist: Array[String]) extends HarvestTypes

case class WhitelistHarvest(endpoint: String, metadataPrefix: String, setlist: Array[String]) extends HarvestTypes

case class AllRecordsHarvest(endpoint: String, metadataPrefix: String) extends HarvestTypes

case class AllSetsHarvest(endpoint: String, metadataPrefix: String) extends HarvestTypes

