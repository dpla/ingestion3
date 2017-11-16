package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

/**
  * Entry point for the data source. Uses OaiConfiguration and the parameters to decide which
  * type of harvest to run, and runs it.
  */

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): OaiRelation = {

    val oaiMethods = new OaiProtocol()

    OaiConfiguration(parameters).getHarvestType match {
      case blackListHarvest: BlacklistHarvest => new BlacklistOaiRelation(blackListHarvest)(oaiMethods)(sqlContext)
      case whitelistHarvest: WhitelistHarvest => new WhitelistOaiRelation(whitelistHarvest)(oaiMethods)(sqlContext)
      case allRecordsHarvest: AllRecordsHarvest => new AllRecordsOaiRelation(allRecordsHarvest)(oaiMethods)(sqlContext)
      case allSetsHarvest: AllSetsHarvest => new AllSetsOaiRelation(allSetsHarvest)(oaiMethods)(sqlContext)
    }
  }
}

