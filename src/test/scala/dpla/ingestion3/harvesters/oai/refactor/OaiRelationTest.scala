package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.sql.SQLContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class OaiRelationTest extends FlatSpec with Matchers with BeforeAndAfter with MockFactory {

  private[this] val oaiMethods = mock[OaiMethods]

  "OaiRelation.getRelation" should "return a BlacklistOaiRelation given a blacklist" in {
    val oaiConfiguration = OaiConfiguration(
      Map(
        "blacklist" -> "eenie,meenie,miney,moe",
        "verb" -> "ListRecords"
      )
    )
    assert(OaiRelation.getRelation(oaiMethods, oaiConfiguration, null).isInstanceOf[BlacklistOaiRelation])
  }

  it should "return a WhitelistOaiRelation given a whitelist" in {
    val oaiConfiguration = OaiConfiguration(
      Map(
        "setlist" -> "eenie,meenie,miney,moe",
        "verb" -> "ListRecords"
      )
    )
    assert(OaiRelation.getRelation(oaiMethods, oaiConfiguration, null).isInstanceOf[WhitelistOaiRelation])
  }

  it should "return a AllSetsOaiRelation given harvestAllSets" in {
    val oaiConfiguration = OaiConfiguration(
      Map(
        "harvestAllSets" -> "true",
        "verb" -> "ListRecords"
      )
    )
    assert(OaiRelation.getRelation(oaiMethods, oaiConfiguration, null).isInstanceOf[AllSetsOaiRelation])

  }

  it should "return a AllRecordsOaiRelation when setlist and blacklist are None and harvestAllSets is false" in {
    val oaiConfiguration = OaiConfiguration(
      Map(
        "harvestAllSets" -> "false",
        "verb" -> "ListRecords"
      )
    )
    assert(OaiRelation.getRelation(oaiMethods, oaiConfiguration, null).isInstanceOf[AllRecordsOaiRelation])
  }




}
